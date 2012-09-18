// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers

function drop(req, cb) {
        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('DELETE FROM %s WHERE _key=\'%s\' ' +
                          'RETURNING _id, _etag',
                          req.bucket.name, req.key);

        log.debug({
                bucket: req.bucket.name,
                key: req.key,
                sql: sql
        }, 'drop: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'drop: failed');
                        cb(err);
                } else if (result.rows.length === 0) {
                        log.debug(err, 'drop: record did not exist');
                        cb(new ObjectNotFoundError(req.bucket.name, req.key));
                } else if (req.etag && result.rows[0]._etag !== req.etag) {
                        log.debug(err, 'drop: etag conflict');
                        cb(new EtagConflictError(req.bucket.name,
                                                 req.key,
                                                 req.etag,
                                                 result.rows[0]._etag));
                } else {
                        req._id = result.rows[0]._id;
                        log.debug(err, 'drop: done');
                        cb();
                }
        });
}


function del(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _del(b, k, opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        bucket: b,
                        key: k,
                        opts: opts
                }, 'delObject: entered');

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'delObject: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'delObject: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        common.loadBucket,
                                        drop,
                                        common.runPostChain
                                ],
                                arg: {
                                        bucket: {
                                                name: b
                                        },
                                        etag: (opts.etag || opts._etag),
                                        key: k,
                                        log: log,
                                        pg: pg,
                                        manatee: manatee,
                                        headers: opts.headers || {}
                                }
                        }, common.pipelineCallback({
                                log: log,
                                name: 'delObject',
                                pg: pg,
                                res: res
                        }));
                });
        }

        return (_del);
}



///--- Exports

module.exports = {
        del: del
};
