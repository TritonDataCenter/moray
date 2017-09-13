/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var util = require('util');

var once = require('once');

var common = require('./common');
var control = require('../control');
var dtrace = require('../dtrace');
var BucketConflictError = require('../errors').BucketConflictError;
var InternalError = require('../errors').InternalError;


///--- Globals

var ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'config', type: 'object' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    common.validateBucket,
    insertConfig,
    createSequence,
    createLockingSerial,
    createTable,
    createIndexes
];


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

    dtrace['createbucket-insertconfig-start'].fire(function () {
        return ([req.msgid, req.req_id, sql]);
    });
    log.debug({
        bucket: util.inspect(bucket)
    }, 'insertConfig: entered');

    q = pg.query(sql, values);

    q.once('error', function (err) {
        log.debug(err, 'insertConfig: failed');
        dtrace['createbucket-insertconfig-error'].fire(function () {
            return ([req.msgid, req.req_id, sql, err.toString()]);
        });
        if (err.code === '23505') {
            cb(new BucketConflictError(err, bucket.name));
        } else {
            cb(new InternalError(err, 'unable to create bucket'));
        }
    });

    q.once('end', function () {
        log.debug('insertConfig: done');
        dtrace['createbucket-insertconfig-done'].fire(function () {
            return ([req.msgid, req.req_id, bucket]);
        });
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

    dtrace['createbucket-createsequence-start'].fire(function () {
        return ([req.msgid, req.req_id, sql]);
    });
    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'createSequence: entered');
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'createSequence: failed');
        dtrace['createbucket-createsequence-error'].fire(function () {
            return ([req.msgid, req.req_id, sql, err.toString()]);
        });
        cb(err);
    });

    q.once('end', function () {
        log.debug('createSequence: done');
        dtrace['createbucket-createsequence-done'].fire(function () {
            return ([req.msgid, req.req_id, bucket]);
        });
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

    dtrace['createbucket-createlockingserial-start'].fire(function () {
        return ([req.msgid, req.req_id, req.bucket.name]);
    });
    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'createLockingSerial: entered');
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'createLockingSerial: failed');
        dtrace['createbucket-createlockingserial-error'].fire(function () {
            return ([req.msgid, req.req_id, sql, err.toString()]);
        });
        cb(err);
    });

    q.once('end', function () {
        sql = util.format('INSERT INTO %s_locking_serial (id) VALUES (1)',
                          bucket.name);
        q = pg.query(sql);
        q.once('error', function (err) {
            log.debug(err, 'createLockingSerial(insert): failed');
            dtrace['createbucket-createlockingserial-error'].fire(function () {
                return ([req.msgid, req.req_id, sql, err.toString()]);
            });
            cb(err);
        });
        q.once('end', function () {
            log.debug('createLockingSerial: done');
            dtrace['createbucket-createlockingserial-done'].fire(function () {
                return ([req.msgid, req.req_id, req.bucket.name]);
            });
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

    dtrace['createbucket-createtable-start'].fire(function () {
        return ([req.msgid, req.req_id, req.bucket.name]);
    });
    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'createTable: entered');
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'createTable: failed');
        dtrace['createbucket-createtable-error'].fire(function () {
            return ([req.msgid, req.req_id, sql, err.toString()]);
        });
        cb(err);
    });

    q.once('end', function () {
        log.debug('createTable: done');
        dtrace['createbucket-createtable-done'].fire(function () {
            return ([req.msgid, req.req_id, req.bucket.name]);
        });
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

    function _creat(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var name = argv[0];
        var cfg = argv[1];
        var opts = argv[2];

        var req = control.buildReq(opts, rpc, options);
        var bucket = {
            name: name,
            index: cfg.index || {},
            pre: cfg.pre || [],
            post: cfg.post || [],
            options: cfg.options || {}
        };
        bucket.options.version = bucket.options.version || 0;
        req.bucket = bucket;

        dtrace['createbucket-start'].fire(function () {
            return ([req.msgid, req.req_id, bucket]);
        });
        req.log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, 'createBucket: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE
        });
    }

    return (_creat);
}



///--- Exports

module.exports = {
    creat: creat
};
