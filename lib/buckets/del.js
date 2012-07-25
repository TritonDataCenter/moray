// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var uuid = require('node-uuid');
var vasync = require('vasync');

require('../errors');



///--- Globals

var sprintf = util.format;

var RESERVED_BUCKETS = ['moray', 'search'];



///--- Handlers

function validate(req, cb) {
        var bucket = req.bucket;
        var log = req.log;

        log.debug('validate: entered (%j)', bucket);
        if (RESERVED_BUCKETS.indexOf(bucket.name) !== -1)
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

        log.debug({ bucket: bucket.name }, 'dropTable: entered');
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


function commit(req, cb) {
        req.pgPool.commit(req.pg, cb);
}


function del(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.pg, 'options.pg');

        var pg = options.pg;

        function _del(opts, bucket, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        opts: opts,
                        bucket: bucket
                }, 'buckets.del: entered');

                pg.start(function (err, client) {
                        log.debug({
                                pg: client
                        }, 'buckets.del: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        validate,
                                        deleteConfig,
                                        dropTable,
                                        commit
                                ],
                                arg: {
                                        bucket: {
                                                name: bucket
                                        },
                                        log: log,
                                        pg: client,
                                        pgPool: pg
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'del: failed');
                                        pg.rollback(client, function () {
                                                res.end(err);
                                        });
                                } else {
                                        log.debug('del: done');
                                        res.end();
                                }
                        });
                });
        }

        return (_del);
}



///--- Exports

module.exports = {
        del: del
};
