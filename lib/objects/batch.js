/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var vasync = require('vasync');
var clone = require('clone');


var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');


///--- Globals

// Operation Pipelines
var put = require('./put');
var update = require('./update');
var del = require('./del');
var deleteMany = require('./del_many');

var ARGS_SCHEMA = [
    { name: 'requests', type: 'requests' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    processRequests
];


///--- Helpers

function mergeOptions(existing, add) {
    var out = clone(existing || {});
    Object.keys(add).forEach(function (key) {
        out[key] = add[key];
    });
    return out;
}


///--- API

function processRequests(req, cb) {
    var etags = [];
    vasync.forEachPipeline({
        inputs: req.requests,
        func: function (r, callback) {
            var subReq = {
                bucket: {
                    name: r.bucket
                },
                log: req.log,
                pg: req.pg,
                req_id: req.req_id,
                rpc: req.rpc,
                bucketCache: req.bucketCache,
                opts: mergeOptions(r.options, req.opts)
            };
            var op = r.operation || 'put';
            r.options = r.options || {};
            var funcPipeline;
            if (op === 'update' || op === 'deleteMany') {
                subReq.rawFilter = r.filter;
                if (r.fields) {
                    subReq.fields = r.fields;
                    subReq.fieldKeys = Object.keys(r.fields);
                }

                if (op === 'update') {
                    funcPipeline = update.pipeline;
                } else {
                    funcPipeline = deleteMany.pipeline;
                }
            } else if (op === 'delete') {
                subReq.key = r.key;
                subReq.etag = r.options.etag;
                funcPipeline = del.pipeline;
            } else {
                subReq.key = r.key;
                subReq.etag = r.options.etag;
                subReq.value = r.value;
                funcPipeline = put.pipeline;
            }

            dtrace['batch-op-start'].fire(function () {
                return ([
                    req.msgid,
                    req.req_id,
                    r.bucket,
                    op,
                    subReq.key || subReq.filter
                ]);
            });

            vasync.pipeline({
                funcs: funcPipeline,
                arg: subReq
            }, function (err) {
                dtrace['batch-op-done'].fire(function () {
                    return ([req.msgid, req.req_id]);
                });

                if (err) {
                    callback(err);
                } else {
                    var out = {
                        bucket: r.bucket,
                        count: subReq._count,
                        etag: subReq._etag
                    };
                    if (r.key) {
                        out.key = r.key;
                    }
                    if (r.filter) {
                        out.filter = r.filter;
                    }

                    etags.push(out);
                    callback();
                }
            });
        }
    }, function (err) {
        if (err) {
            cb(err);
        } else {
            req.etags = etags;
            cb();
        }
    });
}

function batch(options) {
    control.assertOptions(options);

    function _batch(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var requests = argv[0];
        var opts = argv[1];

        var req = control.buildReq(opts, rpc, options);
        req.requests = requests;

        dtrace['batch-start'].fire(function () {
            return ([req.msgid, req.req_id]);
        });
        req.log.debug({
            requests: requests,
            opts: opts
        }, 'batch: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: function () { return {etags: req.etags}; }
        });
    }

    return (_batch);
}


///--- Exports

module.exports = {
    batch: batch
};
