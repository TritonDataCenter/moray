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
        var sql = sprintf('DELETE FROM %s WHERE _key=\'%s\'',
                          req.bucket.name, req.key);
        if (req.etag)
                sql += sprintf(' AND _etag=\'%s\'', req.etag);
        sql += ' RETURNING _id';

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
                                return (res.end(startErr));
                        }

                        log.debug({
                                pg: pg
                        }, 'delObject: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        common.loadBucket,
                                        drop,
                                        common.runPostChain,
                                        common.commit
                                ],
                                arg: {
                                        bucket: {
                                                name: b
                                        },
                                        key: k,
                                        log: log,
                                        pg: pg,
                                        manatee: manatee
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'delObject: failed');
                                        pg.rollback(function () {
                                                res.end(err);
                                        });
                                } else {
                                        log.debug('delObject: done');
                                        res.end();
                                }
                        });

                        return (undefined);
                });
        }

        return (_del);
}



///--- Exports

module.exports = {
        del: del
};
