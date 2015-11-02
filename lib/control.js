/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */



var assert = require('assert-plus');
var vasync = require('vasync');
var libuuid = require('libuuid');

require('./errors');
var pgError = require('./pg').pgError;
var dtrace = require('./dtrace');

///--- API


function assertOptions(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');
    assert.object(options.bucketCache, 'options.bucketCache');
}

function _getPGHandle(route, manatee, params, begin) {
    params = params || {};
    var method = manatee.pg.bind(manatee);
    if (begin) {
        // run pg.begin before returning control
        method = manatee.start.bind(manatee);
    }

    return function getPGHandle(req, cb) {
        method(params, function (startErr, pg) {
            if (startErr) {
                req.log.debug(startErr, '%s: no DB handle', route);
                cb(startErr);
            } else {
                if (req.opts.timeout)
                    pg.setTimeout(req.opts.timeout);
                req.pg = pg;
                cb(null);
            }
        });
    };
}

function releasePGHandle(req) {
    if (req.pg) {
        req.pg.release();
        req.pg = null;
    }
}

function handlerPipeline(options) {
    assert.object(options, 'options');
    assert.string(options.route, 'options.route');
    assert.arrayOfFunc(options.funcs, 'options.funcs');
    assert.object(options.req, 'options.req');
    assert.object(options.req.res, 'options.req.res');
    // Modifiers for res.end() and dtrace.fire() output
    assert.optionalFunc(options.cbOutput, 'options.cbOutput');
    assert.optionalFunc(options.cbProbe, 'options.cbProbe');

    var route = options.route;
    var req = options.req;
    var res = req.res;
    var log = req.log;

    var cbOutput = options.cbOutput || function () { return null; };
    var cbProbe = options.cbProbe || function () { return ([res.msgid]); };

    function pipelineCallback(err) {
        var probe = route.toLowerCase() + '-done';
        function done() {
            if (dtrace[probe]) {
                dtrace[probe].fire(cbProbe);
            }
            // post-pipeline checks
            if (req.pg) {
                assert.fail('PG connection left open');
            }
        }

        if (err) {
            log.debug(err, '%s: failed', route);
            if (req.pg) {
                req.pg.rollback();
                req.pg = null;
            }
            res.end(pgError(err));
            done();
            return;
        }
        req.pg.commit(function (err2) {
            req.pg = null;
            if (err2) {
                log.debug(err2, '%s: failed', route);
                res.end(pgError(err2));
            } else {
                var result = cbOutput();
                if (result) {
                    log.debug({result: result}, '%s: done', route);
                    res.end(result);
                } else {
                    log.debug('%s: done', route);
                    res.end();
                }
            }
            done();
        });
        return;
    }

    vasync.forEachPipeline({
        inputs: options.funcs,
        func: function (handler, cb) {
            dtrace.fire('handler-start', function () {
                return [res.msgid, route, handler.name, req.req_id];
            });
            handler(options.req, function (err) {
                dtrace.fire('handler-done', function () {
                    return [res.msgid, route, handler.name];
                });
                cb(err);
            });
        }
    }, pipelineCallback);
}

function buildReq(opts, res, serverOpts) {
    assert.object(opts);
    assert.object(res);
    assert.object(serverOpts);

    var req = {
        req_id: opts.req_id || libuuid.create(),
        msgid: res.msgid,
        opts: opts,
        res: res,
        bucketCache: serverOpts.bucketCache
    };
    req.log = serverOpts.log.child({
        req_id: req.req_id
    });
    return req;
}

///--- Exports

module.exports = {
    assertOptions: assertOptions,
    getPGHandle: _getPGHandle,
    releasePGHandle: releasePGHandle,
    handlerPipeline: handlerPipeline,
    buildReq: buildReq
};
