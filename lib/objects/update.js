/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var once = require('once');
var libuuid = require('libuuid');

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');

var mod_errors = require('../errors');
var NotIndexedError = mod_errors.NotIndexedError;
var NotNullableError = mod_errors.NotNullableError;
var FieldUpdateError = mod_errors.FieldUpdateError;


// --- Globals

var ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'fields', type: 'object' },
    { name: 'filter', type: 'string' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    common.parseFilter,
    common.loadBucket,
    common.checkOnlyUsableIndexes,
    common.decorateFilter,
    common.buildWhereClause,
    updateRows
];

// This is exported for batch 'update' operations
var SUBPIPELINE = PIPELINE.slice(1);

// --- Handlers

function updateRows(req, cb) {
    var args;
    var b = req.bucket;
    var column  = '';
    var etag = 'u' + libuuid.create().substr(0, 7);
    var i, k;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;
    var vals = req.where.args;

    assert.bool(b.hasExtendedId, 'b.hasExtendedId');

    vals.push(etag + '');
    column += '_etag=$' + vals.length;
    vals.push(Date.now());
    column += ', _mtime=$' + vals.length;

    cb = once(cb);

    try {
        args = common.indexObject(req.bucket.index, req.fields);
    } catch (e) {
        cb(e);
        return;
    }

    var reindexing = common.getReindexingFields(req.bucket);

    for (i = 0; i < req.fieldKeys.length; i++) {
        k = req.fieldKeys[i];
        if (!req.bucket.index[k]) {
            cb(new NotIndexedError(b.name, JSON.stringify(req.fields)));
            return;
        }

        if (reindexing.indexOf(k) !== -1) {
            cb(new NotIndexedError({}, b.name, JSON.stringify(req.fields), {
                unindexedFields: [],
                reindexingFields: reindexing
            }));
            return;
        }

        if (args[k] === null) {
            cb(new NotNullableError(k, req.bucket.index[k].type));
            return;
        }

        vals.push(args[k]);
        column += ',' + k + '=$' + vals.length;
    }

    sql = [];
    if (b.hasExtendedId) {
        /*
         * The "_id" property of Moray objects is potentially virtualised as
         * two underlying columns: "_id" and "_idx".  We use a PostgreSQL
         * Common Table Expression (CTE) to allow us to refer to the update
         * row selection subquery more than once.
         */
        sql.push('WITH row_set AS (SELECT _id, _idx FROM', b.name,
          req.where.clause + ')');
    }
    sql.push('UPDATE', b.name, 'SET', column, 'WHERE');
    if (b.hasExtendedId) {
        sql.push('_id IN (SELECT _id FROM row_set WHERE _id IS NOT NULL) OR',
          '_idx IN (SELECT _idx FROM row_set WHERE _idx IS NOT NULL)');
    } else {
        /*
         * If this bucket does not have the extended ID column, use a basic
         * subquery to select rows to update.
         */
        sql.push('_id IN (SELECT _id FROM', b.name, req.where.clause + ')');
    }
    sql = sql.join(' ');

    req.log.debug({
        bucket: req.bucket.name,
        sql: sql,
        vals: vals
    }, 'updateRows: entered');

    q = pg.query(sql, vals);
    q.once('error', function (err) {
        log.debug(err, 'updateRows: failed');
        cb(err);
    });
    q.once('end', function (res) {
        req._etag = etag;
        req._count = res.rowCount;
        log.debug({
            count: req._count,
            etag: req._etag
        }, 'updateRows: done');
        cb();
    });
}


function update(options) {
    control.assertOptions(options);

    function _update(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var bucket = argv[0];
        var fields = argv[1];
        var filter = argv[2];
        var opts = argv[3];

        var req = control.buildReq(opts, rpc, options);
        req.bucket = {
            name: bucket
        };
        req.fields = fields;
        req.fieldKeys = Object.keys(fields);
        if (req.fieldKeys.length === 0) {
            rpc.fail(new FieldUpdateError(fields));
            return;
        }
        req.rawFilter = filter;

        dtrace['update-start'].fire(function () {
            return ([req.msgid, req.req_id, bucket, fields, filter]);
        });
        req.log.debug({
            bucket: bucket,
            fields: fields,
            filter: filter,
            opts: opts
        }, 'updateObjects: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: common.stdOutput(req)
        });
    }

    return (_update);
}


// --- Exports

module.exports = {
    update: update,
    pipeline: SUBPIPELINE
};
