// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var once = require('once');
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

        cb = once(cb);

        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql = 'INSERT INTO buckets_config ' +
                '(name, index, pre, post, options)' +
                ' VALUES ($1, $2, $3, $4, $5)';
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
        values.push(JSON.stringify(bucket.options || {}));

        log.debug({
                bucket: util.inspect(bucket)
        }, 'insertConfig: entered');

        q = pg.query(sql, values);

        q.once('error', function (err) {
                log.debug(err, 'insertConfig: failed');
                if (err.code === '23505') {
                        cb(new BucketConflictError(err, bucket.name));
                } else {
                        cb(new InternalError(err, 'unable to create bucket'));
                }
        });

        q.once('end', function () {
                log.debug('insertConfig: done');
                cb();
        });

        return (undefined);
}


function createSequence(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql = sprintf('CREATE SEQUENCE %s_serial', bucket.name);

        cb = once(cb);

        log.debug({
                bucket: bucket.name,
                sql: sql
        }, 'createSequence: entered');
        q = pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'createSequence: failed');
                cb(err);
        });

        q.once('end', function () {
                log.debug('createSequence: done');
                cb();
        });
}


function createLockingSerial(req, cb) {
        if (req.bucket.options.guaranteeOrder !== true) {
                cb();
                return;
        }

        cb = once(cb);

        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql = sprintf('CREATE TABLE %s_locking_serial (' +
                          'id INTEGER PRIMARY KEY)',
                          bucket.name);

        log.debug({
                bucket: bucket.name,
                sql: sql
        }, 'createLockingSerial: entered');
        q = pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'createLockingSerial: failed');
                cb(err);
        });

        q.once('end', function () {
                sql = sprintf('INSERT INTO %s_locking_serial (id) VALUES (1)',
                              bucket.name);
                q = pg.query(sql);
                q.once('error', function (err) {
                        log.debug(err, 'createLockingSerial(insert): failed');
                        cb(err);
                });
                q.once('end', function () {
                        log.debug('createLockingSerial: done');
                        cb();
                });
        });
}


function createTable(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql = sprintf('CREATE TABLE %s (' +
                          '_id INTEGER DEFAULT nextval(\'%s_serial\'), ' +
                          '_txn_snap INTEGER, ' +
                          '_key TEXT PRIMARY KEY, ' +
                          '_value TEXT NOT NULL, ' +
                          '_etag CHAR(8) NOT NULL, ' +
                          '_mtime BIGINT NOT NULL, ' +
                          '_vnode BIGINT ' +
                          '%s)',
                          bucket.name,
                          bucket.name,
                          common.buildIndexString(bucket.index));

        cb = once(cb);

        log.debug({
                bucket: bucket.name,
                sql: sql
        }, 'createTable: entered');
        q = pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'createTable: failed');
                cb(err);
        });

        q.once('end', function () {
                log.debug('createTable: done');
                cb();
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
                }).concat('_id', '_etag', '_mtime', '_vnode')
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

                var bucket = {
                        name: name,
                        index: cfg.index || {},
                        pre: cfg.pre || [],
                        post: cfg.post || [],
                        options: cfg.options || {}
                };

                bucket.options.version = bucket.options.version || 0;

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr,
                                          'createBucket: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        if (opts.timeout)
                                pg.setTimeout(opts.timeout);

                        log.debug({
                                pg: pg
                        }, 'createBucket: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        common.validateBucket,
                                        insertConfig,
                                        createSequence,
                                        createLockingSerial,
                                        createTable,
                                        createIndexes
                                ],
                                arg: {
                                        bucket: bucket,
                                        log: log,
                                        pg: pg,
                                        manatee: manatee
                                }
                        }, common.pipelineCallback({
                                log: log,
                                name: 'createBucket',
                                pg: pg,
                                res: res
                        }));
                });
        }

        return (_creat);
}



///--- Exports

module.exports = {
        creat: creat
};
