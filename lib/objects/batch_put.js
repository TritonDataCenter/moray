// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var uuid = require('node-uuid');
var vasync = require('vasync');


var dtrace = require('../dtrace');
var common = require('./common');
var put = require('./put');
require('../errors');



///--- Globals

var sprintf = util.format;
var pgError = common.pgError;



///--- API

function batchPut(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;
        var startProbe = dtrace['batchput-start'];
        var doneProbe = dtrace['batchput-done'];

        function _batchPut(requests, opts, res) {
                startProbe.fire(function () {
                        return ([res.msgid]);
                });

                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        requests: requests,
                        opts: opts
                }, 'batchPut: entered');

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'batchPut: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'batchPut: transaction started');

                        var etags = [];
                        var i = -1;

                        function commit() {
                                pg.commit(function (err) {
                                        if (err) {
                                                log.debug({
                                                        err: err
                                                }, 'batchPut: failed');
                                                res.end(pgError(err));
                                        } else {
                                                log.debug('batchPut: done');
                                                res.end({
                                                        etags: etags
                                                });
                                        }

                                        doneProbe.fire(function () {
                                                return ([res.msgid]);
                                        });
                                });
                        }

                        function next() {
                                var r = requests[++i];
                                if (!r) {
                                        commit();
                                        return;
                                }

                                r.options = r.options || {};
                                vasync.pipeline({
                                        funcs: put.pipeline,
                                        arg: {
                                                bucket: {
                                                        name: r.bucket
                                                },
                                                etag: r.options.etag,
                                                key: r.key,
                                                log: log,
                                                pg: pg,
                                                manatee: manatee,
                                                res: res,
                                                value: r.value,
                                                _value: r.options._value,
                                                headers: r.options.headers || {}
                                        }
                                }, function doneOne(err) {
                                        if (err) {
                                                pg.rollback();
                                                res.end(pgError(err));
                                        } else {
                                                etags.push({
                                                        bucket: r.bucket,
                                                        key: r.key,
                                                        etag: res._etag
                                                });
                                                next();
                                        }
                                });
                        }

                        next();
                });
        }

        return (_batchPut);
}



///--- Exports

module.exports = {
        batchPut: batchPut
};
