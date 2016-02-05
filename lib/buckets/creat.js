/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var once = require('once');

var common = require('./common');
var control = require('../control');
var BucketConflictError = require('../errors').BucketConflictError;


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
    var sql = util.format('CREATE SEQUENCE %s_serial', bucket.name);

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
    var sql = util.format(('CREATE TABLE %s_locking_serial ' +
                           '(id INTEGER PRIMARY KEY)'),
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
        sql = util.format('INSERT INTO %s_locking_serial (id) VALUES (1)',
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
    var sql = util.format(('CREATE TABLE %s (' +
                           '_id INTEGER DEFAULT nextval(\'%s_serial\'), ' +
                           '_txn_snap INTEGER, ' +
                           '_key TEXT PRIMARY KEY, ' +
                           '_value TEXT NOT NULL, ' +
                           '_etag CHAR(8) NOT NULL, ' +
                           '_mtime BIGINT DEFAULT CAST ' +
                           '(EXTRACT (EPOCH FROM NOW()) * 1000 AS BIGINT), ' +
                           '_vnode BIGINT ' +
                           '%s)'),
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
            .map(common.mapIndexType.bind(null, req.bucket.index))
    }, cb);
}

///--- API

function creat(options) {
    control.assertOptions(options);
    var route = 'createBucket';

    function _creat(name, cfg, opts, res) {
        var req = control.buildReq(opts, res, options);
        var bucket = {
            name: name,
            index: cfg.index || {},
            pre: cfg.pre || [],
            post: cfg.post || [],
            options: cfg.options || {}
        };
        bucket.options.version = bucket.options.version || 0;
        req.bucket = bucket;

        req.log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, route + ': entered');

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee, {}, true),
                common.validateBucket,
                insertConfig,
                createSequence,
                createLockingSerial,
                createTable,
                createIndexes
            ]
        });
    }

    return (_creat);
}



///--- Exports

module.exports = {
    creat: creat
};
