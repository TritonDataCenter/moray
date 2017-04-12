/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');
var errors = require('../errors');

var HANDLED_FINDOBJECTS_OPTIONS = [
    'req_id',
    'limit',
    'offset',
    'sort',
    'noLimit',
    'no_count',
    'sql_only',
    'noBucketCache',
    'timeout',
    'requireIndexes'
];

///--- Handlers

/*
 * Makes sure that all fields used in the findObjects request "req" have indexes
 * that are usable and calls the function "cb". If at least one field has an
 * underlying index that is not usable, "cb" is called with a NotIndexedError
 * object as its first parameter.
 */
function checkRequiredIndexes(req, cb) {
    var log = req.log;
    var unusableIndexes;

    if (req.opts && req.opts.requireIndexes === true) {
        unusableIndexes =
            common.getUnusableIndexes(req.filter, req.bucket, log);

        assert.object(unusableIndexes, 'unusableIndexes');
        assert.arrayOfString(unusableIndexes.unindexedFields,
            'unusableIndexes.unindexedFields');
        assert.arrayOfString(unusableIndexes.reindexingFields,
            'unusableIndexes.reindexingFields');

        if (unusableIndexes.unindexedFields.length > 0 ||
            unusableIndexes.reindexingFields.length > 0) {
            log.error('filter uses unusable indexes');
            cb(new errors.NotIndexedError({}, req.bucket.name, req.rawFilter, {
                unindexedFields: unusableIndexes.unindexedFields,
                reindexingFields: unusableIndexes.reindexingFields
            }));
            return;
        } else {
            log.trace('filter does not use unusable indexes');
        }
    }

    cb();
}

/*
 * Sends a record that contains the list of findObjects request options that the
 * server handles. This allows moray clients to compare this set of handled
 * options with what they expect the server to handle.
 */
function sendHandledOptions(req, cb) {
    var res = req.res;

    if (req.opts && req.opts.internalOpts &&
        req.opts.internalOpts.sendHandledOptions === true) {
        res.write({_handledOptions: HANDLED_FINDOBJECTS_OPTIONS});
    }

    cb();
}

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
        }, 'getCount: row found');

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

            query.removeAllListeners('end');
            query.removeAllListeners('row');
            query.removeAllListeners('error');

            cb(e);
            return;
        }

        var v = obj.value;
        /*
         * Adding these properties on "v" is required so that search filters
         * such as (_mtime>=x) and more generally any filter using records'
         * metadata that is not part of the objects' value can match the filter
         * "filter".
         */
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
        req.res.write(req.opts._sql);
    }
    cb(null);
}

function find(options) {
    control.assertOptions(options);
    var route = 'findObjects';

    function _find(bucket, filter, opts, res) {
        var req = control.buildReq(opts, res, options);
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
                checkRequiredIndexes,
                sendHandledOptions,
                common.decorateFilter,
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
