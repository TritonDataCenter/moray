// Copyright 2013 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var ldap = require('ldapjs');
var uuid = require('node-uuid');
var vasync = require('vasync');


var dtrace = require('../dtrace');
var common = require('./common');
var put = require('./put');
var update = require('./update');
var del = require('./del');
var deleteMany = require('./del_many');
require('../errors');



///--- Globals

var sprintf = util.format;
var pgError = common.pgError;



///--- API

function batch(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;
    var startProbe = dtrace['batch-start'];
    var doneProbe = dtrace['batch-done'];
    var opStartProbe = dtrace['batch-op-start'];
    var opDoneProbe = dtrace['batch-op-done'];

    function _batch(requests, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        startProbe.fire(function () {
            return ([res.msgid, id]);
        });

        log.debug({
            requests: requests,
            opts: opts
        }, 'batch: entered');

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'batch: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'batch: transaction started');

            var etags = [];
            var i = -1;

            function commit() {
                pg.commit(function (err) {
                    if (err) {
                        log.debug({
                            err: err
                        }, 'batch: failed');
                        res.end(pgError(err));
                    } else {
                        log.debug('batch: done');
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

                var arg = {
                    bucket: {
                        name: r.bucket
                    },
                    log: log,
                    pg: pg,
                    manatee: manatee,
                    req_id: id,
                    res: res,
                    opts: r.options || opts || {}
                };
                r.options = r.options || {};
                var funcs;
                var op = r.operation || 'put';
                if (op === 'update' || op === 'deleteMany') {
                    var filter;
                    try {
                        /* JSSTYLED */
                        filter = ldap.parseFilter(r.filter);
                    } catch (e) {
                        log.debug({
                            err: e
                        }, 'bad search filter');
                        pg.rollback();
                        res.end(e);
                        return;
                    }

                    arg.filter = filter;
                    if (r.fields) {
                        arg.fields = r.fields;
                        arg.fieldKeys =
                            Object.keys(r.fields);
                    }
                    arg.opts.noLimit = true;

                    if (op === 'update') {
                        funcs = update.pipeline;
                    } else {
                        funcs = deleteMany.pipeline;
                    }
                } else if (op === 'delete') {
                    arg.key = r.key;
                    arg.etag = r.options.etag;
                    arg.headers = r.options.headers || {};
                    funcs = del.pipeline;
                } else {
                    r.options = r.options || {};
                    arg.etag = r.options.etag;
                    arg.key = r.key;
                    arg.value = r.value;
                    arg._value = (r.options._value ||
                                  JSON.stringify(r.value));
                    arg.headers = r.options.headers || {};
                    funcs = put.pipeline;
                }

                opStartProbe.fire(function () {
                    return ([
                        res.msgid,
                        id,
                        arg.bucket.name,
                        op,
                        arg.key || arg.filter
                    ]);
                });

                vasync.pipeline({
                    funcs: funcs,
                    arg: arg
                }, function doneOne(err) {
                    opDoneProbe.fire(function () {
                        return ([res.msgid]);
                    });

                    if (err) {
                        pg.rollback();
                        res.end(pgError(err));
                    } else {
                        var _tmp = {
                            bucket: r.bucket,
                            count: res._count,
                            etag: res._etag
                        };
                        if (r.key)
                            _tmp.key = r.key;
                        if (r.filter)
                            _tmp.filter = r.filter;

                        etags.push(_tmp);
                        next();
                    }
                });
            }

            next();
        });
    }

    return (_batch);
}



///--- Exports

module.exports = {
    batch: batch
};
