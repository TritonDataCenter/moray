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

function insertConfig(req, cb) {
        if (req.prev)
                return (cb());

        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = 'INSERT INTO buckets_config (name, index, pre, post)' +
                ' VALUES ($1, $2, $3, $4)';
        var values = [
                bucket.name,
                JSON.stringify(bucket.index)
        ];
        values.push(JSON.stringify((bucket.pre || []).map(function (f) {
                return (f.toString());
        })));
        values.push(JSON.stringify((bucket.post || []).map(function (f) {
                return (f.toString());
        })));

        log.debug({
                bucket: bucket
        }, 'insertConfig: entered');
        pg.query(sql, values, function (err, result) {
                if (err) {
                        log.debug({
                                bucket: bucket.name,
                                err: err
                        }, 'insertConfig: failed');
                        return (cb(err));
                }

                log.debug({
                        bucket: bucket.name,
                }, 'insertConfig: done');
                return (cb());
        });
}


function createTable(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('CREATE TABLE %s (' +
                          '_id SERIAL, ' +
                          '_key TEXT PRIMARY KEY, ' +
                          '_value TEXT NOT NULL, ' +
                          '_etag CHAR(8) NOT NULL, ' +
                          '_mtime BIGINT NOT NULL%s)',
                          bucket.name,
                          common.buildIndexString(bucket.index))

        log.debug({
                bucket: bucket.name,
                sql: sql
        }, 'createTable: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'createTable: failed');
                        cb(err);
                } else {
                        log.debug('createTable: done');
                        cb();
                }
        });
}


function createIndexes(req, cb) {
        // we can skip unique indexes, as those implicitly have a PG index
        // at create table time
        common.createIndexes({
                bucket: req.bucket.name,
                log: req.log,
                pg: req.pg,
                indexes: Object.keys(req.bucket.index).filter(function (k) {
                        return (!req.bucket.index[k].unique);
                }).concat('_id', '_etag', '_mtime')
        }, cb);
}


function creat(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _creat(name, cfg, opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        bucket: name,
                        cfg: cfg,
                        opts: opts
                }, 'createBucket: entered');

                manatee.start(function (err, pg) {
                        if (err) {
                                log.debug(err, 'createBucket: no DB handle');
                                return (res.end(err));
                        }

                        log.debug({
                                pg: pg
                        }, 'createBucket: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        common.validateBucket,
                                        insertConfig,
                                        createTable,
                                        createIndexes,
                                        common.commit
                                ],
                                arg: {
                                        bucket: {
                                                name: name,
                                                index: cfg.index || {},
                                                pre: cfg.pre || [],
                                                post: cfg.post || []
                                        },
                                        log: log,
                                        pg: pg,
                                        manatee: manatee
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'createBucket: failed');
                                        pg.rollback(function () {
                                                res.end(err);
                                        });
                                } else {
                                        log.debug('createBucket: done');
                                        res.end();
                                }
                        });
                });
        }

        return (_creat);
}



///--- Exports

module.exports = {
        creat: creat
};
