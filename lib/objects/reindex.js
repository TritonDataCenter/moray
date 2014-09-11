/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var vasync = require('vasync');


var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');


///--- Handlers

function requiresReindex(req, cb) {
    var active = req.bucket.reindex_active;
    if (active && Object.keys(active).length > 0) {
        req.requiresReindex = true;
    }

    // verify count argument
    if (req.count <= 0) {
        var err = new Error(util.format('count %d < 1', req.count));
        err.name = 'InvalidArgumentError';
        cb(err);
        return;
    }

    req._processed = 0;
    req._updateRows = [];
    cb(null);
}


function processRows(req, cb) {
    if (!req.requiresReindex) {
        cb(null);
        return;
    }

    var log = req.log;

    var sql = util.format(('SELECT *, \'%s\' AS req_id FROM %s ' +
                           'WHERE _rver IS NULL OR _rver < %d ' +
                           'LIMIT %d FOR UPDATE'),
                          req.req_id, req.bucket.name,
                          req.bucket.options.version,
                          req.count);

    log.debug({
        bucket: req.bucket.name,
        count: req.count
    }, 'reindexRows: entered');

    var version = parseInt(req.bucket.options.version, 10);

    function indexObject(obj, callback) {
        var index = common.indexObject(req.bucket.index, obj.value);
        var fields = [];
        var values = [];

        // set _value to merge any changes picked up from updateObjects
        fields.push('_value');
        values.push(obj.value);
        Object.keys(req.bucket.index).forEach(function (k) {
            fields.push(k);
            values.push(index[k]);
        });
        fields.push('_rver');
        values.push(version);

        var fieldSet = fields.map(function (field, idx) {
            return util.format('%s = $%d', field, idx + 1);
        }).join(', ');

        values.push(obj.key);
        var updateSql = util.format(('UPDATE %s SET %s ' +
                                     'WHERE _key=$%d ' +
                                     'RETURNING _id,  \'%s\' AS req_id'),
                                    req.bucket.name,
                                    fieldSet,
                                    values.length,
                                    req.req_id);
        log.trace({
            bucket: req.bucket.name,
            version: version,
            key: obj.key
        }, 'indexObject: entered');
        dtrace['reindexobjects-record'].fire(function () {
            return ([req.msgid, req.bucket.name, obj.key]);
        });


        var q = req.pg.query(updateSql, values);
        q.once('error', function (err) {
            log.trace(err, 'indexObject: failed');
            callback(err);
        });
        q.once('end', function () {
            log.trace('indexObject: done');
            req._processed++;
            callback(null);
        });
    }

    // There is no backpressure on rows being returned from the SELECT
    // statement so heap consumption my be extreme if an unreasonable page size
    // is chosen.
    var result = req.pg.query(sql);
    var queue = vasync.queue(indexObject, 1);


    // Ignore field being reindexed when performing row->obj conversion.
    // This prevents empty/stale fields from clobbering good data.
    var ignore = [];
    if (req.bucket.reindex_active) {
        Object.keys(req.bucket.reindex_active).forEach(function (ver) {
            ignore = ignore.concat(req.bucket.reindex_active[ver]);
        });
    }

    result.on('row', function (row) {
        var obj = common.rowToObject(req.bucket, row, ignore);
        queue.push(obj);
    });
    result.once('error', function (err) {
        log.debug(err, 'reindexRows: failed');
        cb(err);
    });
    result.once('end', queue.close.bind(queue));

    queue.once('end', function () {
        log.debug('reindexRows: done');
        cb(null);
    });
}


function recordStatus(req, cb) {
    if (!req.requiresReindex) {
        cb(null);
        return;
    }

    var log = req.log;
    var pg = req.pg;

    if (req._processed === 0) {
        // Clear reindex_active if all rows are up to date
        var sql = 'UPDATE buckets_config SET reindex_active = \'{}\' ' +
                  'WHERE name = $1';
        log.debug({
            bucket: req.bucket.name
        }, 'recordStatus: entered');
        var q = pg.query(sql, [req.bucket.name]);
        q.on('error', function (err) {
            log.debug('recordStatus: failed');
            cb(err);
        });
        q.on('end', function () {
            log.debug('recordStatus: done');
            cb(null);
        });
    } else {
        cb(null);
    }
}


function countRemaining(req, cb) {
    if (!req.requiresReindex || req.opts.no_count) {
        cb(null);
        return;
    }

    var log = req.log;
    var pg = req.pg;

    var sql = util.format(('SELECT COUNT(*) AS _count FROM %s ' +
                           'WHERE _rver IS NULL OR _rver < %d'),
                          req.bucket.name,
                          parseInt(req.bucket.options.version, 10));
    log.debug({
        bucket: req.bucket.name,
        sql: sql
    }, 'countRemaining: entered');

    var q = pg.query(sql);

    q.on('error', function (err) {
        q.removeAllListeners('end');
        q.removeAllListeners('row');
        log.debug(err, 'query error');
        cb(err);
    });
    q.on('row', function (row) {
        log.debug({
            row: row
        }, 'countRemaining: row found');

        req._countRemaining = parseInt(row._count, 10);
    });
    q.on('end', function () {
        q.removeAllListeners('error');
        q.removeAllListeners('row');
        log.debug('countRemaining: done');
        cb(null);
    });
}



function reindex(options) {
    control.assertOptions(options);
    var route = 'reindexObjects';

    function _reindex(bucket, count, opts, res) {
        var req = control.buildReq(opts, res, options.log);
        req.count = parseInt(count, 10);
        req.bucket = {
            name: bucket
        };
        // Forcibly disable bucket cache
        req.opts.noBucketCache = true;

        dtrace['reindexobjects-start'].fire(function () {
            return ([res.msgid, req.req_id, bucket, count]);
        });
        req.log.debug({
            bucket: bucket,
            count: count,
            opts: opts
        }, '%s: entered', route);

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee),
                common.loadBucket,
                requiresReindex,
                processRows,
                recordStatus,
                countRemaining
            ],
            cbOutput: function () {
                var result = { processed: req._processed };
                if (req._countRemaining !== undefined) {
                    result.remaining = req._countRemaining;
                }
                return result;
            }
        });
    }

    return (_reindex);
}


///--- Exports

module.exports = {
    reindex: reindex
};
