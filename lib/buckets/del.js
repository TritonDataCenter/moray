// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers

function validate(req, cb) {
        var bucket = req.bucket;
        var log = req.log;

        log.debug('validate: entered (%j)', bucket);
        if (common.RESERVED_BUCKETS.indexOf(bucket.name) !== -1)
                return (cb(new InvalidBucketNameError(bucket)));

        log.debug('validate: done');
        return (cb());
}


function deleteConfig(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('DELETE FROM buckets_config WHERE name = \'%s\'',
                          bucket.name);

        log.debug({ bucket: bucket.name }, 'deleteConfig: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'deleteConfig: failed');
                        cb(err);
                } else {
                        log.debug('deleteConfig: done');
                        cb();
                }
        });
}


function dropTable(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('DROP TABLE %s', bucket.name);

        log.debug({
                bucket: bucket.name,
                sql: sql
        }, 'dropTable: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'dropTable: failed');
                        cb(err);
                } else {
                        log.debug('dropTable: done');
                        cb();
                }
        });
}


function del(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _del(bucket, opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        bucket: bucket,
                        opts: opts
                }, 'delBucket: entered');

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'delBucket: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'delBucket: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        validate,
                                        deleteConfig,
                                        dropTable
                                ],
                                arg: {
                                        bucket: {
                                                name: bucket
                                        },
                                        log: log,
                                        pg: pg,
                                        manatee: manatee
                                }
                        }, common.pipelineCallback({
                                log: log,
                                name: 'delBucket',
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
