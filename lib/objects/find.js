/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Handlers

function beginRepeatableRead(req, cb) {
    req.pg.begin('REPEATABLE READ', cb);
}

function getCount(req, cb) {
    if (req.opts.no_count) {
        cb();
        return;
    }

    var bucket = req.bucket.name;
    var log = req.log;
    var sql = util.format(('SELECT count(1) over () as _count, ' +
                           '\'%s\' AS req_id FROM %s %s'),
                          req.req_id, bucket, req.where.clause);

    if (req.opts.sql_only) {
        req.opts._sql = {
            count: sql
        };
        cb();
        return;
    }

    log.debug({
        bucket: req.bucket.name,
        key: req.key,
        sql: sql,
        args: req.where.args
    }, 'getCount: entered');

    var query = req.pg.query(sql, req.where.args);
    query.on('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('row');
        log.debug(err, 'query error');
        cb(err);
    });

    query.on('row', function (row) {
        log.debug({
            row: row
        }, 'getRecords: row found');

        req._count = parseInt(row._count, 10);
    });

    query.on('end', function () {
        query.removeAllListeners('error');
        query.removeAllListeners('row');
        log.debug('getCount: done');
        cb();
    });
}

function getRecords(req, cb) {
    var bucket = req.bucket.name;
    var filter = req.filter;
    var log = req.log;
    var res = req.res;
    var sql = util.format('SELECT *, \'%s\' AS req_id FROM %s %s',
                          req.req_id, bucket, req.where.clause);

    if (req.opts.sql_only) {
        if (!req.opts._sql)
            req.opts._sql = {};
        req.opts._sql.query = sql;
        req.opts._sql.args = req.where.args;
        cb();
        return;
    }

    log.debug({
        bucket: req.bucket.name,
        key: req.key,
        sql: sql,
        args: req.where.args
    }, 'getRecords: entered');

    var query = req.pg.query(sql, req.where.args);
    query.on('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('row');
        log.debug(err, 'query error');
        cb(err);
    });

    query.on('row', function (row) {
        log.debug({
            row: row
        }, 'getRecords: row found');

        // MANTA-317: we need to check that the object actually matches
        // the real filter, which requires us to do this janky stub out
        // of _id and _mtime
        var obj;

        try {
            obj = common.rowToObject(req.bucket, row);
        } catch (e) {
            log.error({
                bucket: req.bucket.name,
                key: req.key,
                sql: sql,
                args: req.where.args,
                err: e,
                row: row
            }, 'find: error translating row to JS object');

            res.removeAllListeners('end');
            res.removeAllListeners('row');
            res.removeAllListeners('error');

            cb(e);
        }

        var v = obj.value;
        v._id = obj._id;
        v._txn_snap = obj._txn_snap;
        v._etag = obj._etag;
        v._key = obj.key;
        v._mtime = obj._mtime;
        v._count = obj._count = req._count;

        if (filter.matches(v)) {
            delete v._id;
            delete v._txn_snap;
            delete v._etag;
            delete v._key;
            delete v._mtime;
            delete v._count;
            res.write(obj);
            dtrace['findobjects-record'].fire(function () {
                return ([res.msgid,
                         obj.key,
                         obj._id,
                         obj._etag,
                         row._value]);
            });
            res._num_records++;
        }
    });

    query.on('end', function () {
        query.removeAllListeners('error');
        query.removeAllListeners('row');
        log.debug('getRecords: done');
        cb();
    });
}

function sendSQL(req, cb) {
    if (req.opts.sql_only) {
        req.res.write(req._sql);
    }
    cb(null);
}

function find(options) {
    control.assertOptions(options);
    var route = 'findObjects';

    function _find(bucket, filter, opts, res) {
        var req = control.buildReq(opts, res, options.log);
        req.bucket = {
            name: bucket
        };
        req.rawFilter = filter;
        req.res._num_records = 0;

        dtrace['findobjects-start'].fire(function () {
            return ([res.msgid, req.req_id, bucket, filter]);
        });
        req.log.debug({
            bucket: bucket,
            filter: filter,
            opts: opts
        }, '%s: entered', route);

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                common.parseFilter,
                control.getPGHandle(route, options.manatee, {
                    async: false,
                    read: true
                }),
                beginRepeatableRead,
                common.loadBucket,
                common.buildWhereClause,
                getCount,
                getRecords,
                sendSQL
            ],
            cbOutput: function () { return; },
            cbProbe: function () { return [res.msgid, res._num_records]; }
        });
    }

    return (_find);
}



///--- Exports

module.exports = {
    find: find
};
