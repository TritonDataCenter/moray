/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */


var assert = require('assert-plus');
var vasync = require('vasync');
var libuuid = require('libuuid');

var mod_errors = require('./errors');
var mod_schema = require('./schema');

var InvocationError = mod_errors.InvocationError;
var pgError = require('./pg').pgError;
var dtrace = require('./dtrace');

// --- Globals

var SCHEMA_TO_DESCR = {
    'integer': 'a nonnegative integer',
    'string': 'a nonempty string',
    'requests': 'an array of valid request objects',
    'array': 'an array',
    'object': 'an object',
    'options': 'a valid options object'
};


// --- API


function assertOptions(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');
    assert.object(options.bucketCache, 'options.bucketCache');
}


function _getPGHandleAfter(req, cb) {
    function done(startErr, pg) {
        if (startErr) {
            req.log.debug(startErr, '%s: no DB handle', req.route);
            cb(startErr);
        } else {
            if (req.opts.hasOwnProperty('timeout')) {
                pg.setTimeout(req.opts.timeout);
            }
            pg.setRequestId(req.req_id);
            req.pg = pg;
            cb(null);
        }
    }

    return done;
}


function getPGHandle(req, cb) {
    req.manatee.pg(_getPGHandleAfter(req, cb));
}


function getPGHandleAndTransaction(req, cb) {
    req.manatee.pg(_getPGHandleAfter(req, function (err) {
        if (err) {
            cb(err);
            return;
        }

        req.pg.begin(function (bErr) {
            if (bErr) {
                req.log.debug({
                    pg: req.pg,
                    err: err
                }, 'getPGHandleAndTransaction: BEGIN failed');
                cb(bErr);
                return;
            }
            cb(null);
        });
    }));
}


function handlerPipeline(options) {
    assert.object(options, 'options');
    assert.arrayOfFunc(options.funcs, 'options.funcs');
    assert.object(options.req, 'options.req');
    assert.object(options.req.rpc, 'options.req.rpc');
    assert.string(options.req.route, 'options.req.route');
    // Modifiers for rpc.end() and dtrace.fire() output
    assert.optionalFunc(options.cbOutput, 'options.cbOutput');
    assert.optionalFunc(options.cbProbe, 'options.cbProbe');

    var req = options.req;
    var route = req.route;
    var rpc = req.rpc;
    var log = req.log;

    var cbOutput = options.cbOutput || function () { return null; };
    var cbProbe = options.cbProbe || function () {
        return ([req.msgid, req.req_id]);
    };

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
            rpc.fail(pgError(err));
            done();
            return;
        }

        req.pg.commit(function (err2) {
            req.pg = null;
            if (err2) {
                log.debug(err2, '%s: failed', route);
                rpc.fail(pgError(err2));
            } else {
                var result = cbOutput();
                if (result) {
                    log.debug({result: result}, '%s: done', route);
                    rpc.end(result);
                } else {
                    log.debug('%s: done', route);
                    rpc.end();
                }
            }
            done();
        });
    }

    req.pipeline = vasync.forEachPipeline({
        inputs: options.funcs,
        func: function (handler, cb) {
            dtrace.fire('handler-start', function () {
                return [req.msgid, route, handler.name, req.req_id];
            });
            handler(options.req, function (err) {
                if (err) {
                    dtrace.fire('handler-error', function () {
                        return ([req.msgid, req.req_id, route, handler.name,
                            err.toString()]);
                    });
                }
                dtrace.fire('handler-done', function () {
                    return [req.msgid, route, handler.name, req.req_id];
                });
                cb(err);
            });
        }
    }, pipelineCallback);
}

function buildReq(opts, rpc, serverOpts) {
    assert.object(opts);
    assert.object(rpc);
    assert.object(serverOpts);

    /*
     * Note that the 'msgid' below is the request ID generated by and for the
     * Fast protocol, and unrelated to the 'req_id' sent by Moray consumers
     * to help identify related requests across different Triton/Manta services.
     *
     * The 'connId' and 'msgid' aren't used by Moray (except in DTrace probes),
     * but are attached to the logger and to the req object to help correlate
     * log messages and objects in crash dumps. The names are chosen to match
     * the same names used in Fast logs for easier analysis.
     */
    var connid = rpc.connectionId();
    var msgid = rpc.requestId();
    var route = rpc.methodName();
    var reqid = opts.req_id || libuuid.create();

    var log = serverOpts.log.child({
        connId: connid,
        msgid: msgid,
        route: route,
        req_id: reqid
    }, true);

    var req = {
        log: log,
        req_id: reqid,
        route: route,
        connId: connid,
        msgid: msgid,
        opts: opts,
        rpc: rpc,
        manatee: serverOpts.manatee,
        bucketCache: serverOpts.bucketCache

        /*
         * A number of RPCs add their own fields to this object. Some common
         * ones used across a number of RPCs are:
         *
         * - "pg", the Manatee client
         * - "pipeline", the vasync status object for the RPC's operations
         * - "bucket", the configuration of the affected/searched bucket
         * - "key", the key being affected by this RPC
         * - "where", an object representing the WHERE clause for a SQL query,
         *   containing two fields:
         *
         *     - "clause", the actual "WHERE ..." to use in the SQL string
         *     - "args", the values needed by the clause's $1, $2, etc.
         *
         * - "_etag", the new etag for objects affected by the RPC
         * - "_count", the number of objects touched by an RPC
         *
         * Not everything is listed here, since each RPC also has its own unique
         * set of fields that it stores on this object.
         */
    };

    return req;
}


/*
 * Validate that the correct number of arguments are provided to an RPC, and are
 * of the expected type. If the arguments are incorrect, then this function will
 * handle failing the RPC with an appropriate error, and return 'true'. If the
 * arguments are okay, then the function will return 'false', and the RPC can
 * continue normally.
 *
 * - rpc: The node-fast rpc object
 * - argv: The array of arguments provided to the RPC (obtained from rpc.argv())
 * - types: An array of { name, type } objects describing the arguments expected
 *   by this RPC, and their types
 *
 * Valid types to check for are:
 *
 * - array: Check that the argument is an Array
 * - string: Check that the argument is a nonempty String
 * - integer: Check that the argument is a nonnegative integer.
 * - object: Check that the argument is a non-null object.
 */
function invalidArgs(rpc, argv, types) {
    var route = rpc.methodName();
    var len = types.length;

    if (argv.length !== len) {
        rpc.fail(new InvocationError(
            '%s expects %d argument%s', route, len, len === 1 ? '' : 's'));
        return true;
    }

    for (var i = 0; i < len; i++) {
        var name = types[i].name;
        var type = types[i].type;
        var val = argv[i];

        var err = mod_schema.validateArgument(name, type, val);
        if (err !== null) {
            rpc.fail(new InvocationError(err,
                '%s expects "%s" (args[%d]) to be %s',
                route, name, i, SCHEMA_TO_DESCR[type]));
            return true;
        }
    }

    return false;
}


// --- Exports

module.exports = {
    assertOptions: assertOptions,
    getPGHandle: getPGHandle,
    getPGHandleAndTransaction: getPGHandleAndTransaction,
    handlerPipeline: handlerPipeline,
    invalidArgs: invalidArgs,
    buildReq: buildReq
};
