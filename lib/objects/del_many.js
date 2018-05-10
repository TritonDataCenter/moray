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

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');


// --- Globals

var ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
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
    drop
];

// This is exported for batch 'deleteMany' operations
var SUBPIPELINE = PIPELINE.slice(1);


// --- Handlers

function drop(req, cb) {
    cb = once(cb);

    var b = req.bucket.name;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;

    assert.bool(req.bucket.hasExtendedId, 'req.bucket.hasExtendedId');

    sql = [];
    if (req.bucket.hasExtendedId) {
        /*
         * The "_id" property of Moray objects is potentially virtualised as
         * two underlying columns: "_id" and "_idx".  We use a PostgreSQL
         * Common Table Expression (CTE) to allow us to refer to the delete
         * row selection subquery more than once.
         */
        sql.push('WITH row_set AS (SELECT _id, _idx FROM', b,
          req.where.clause + ')');
    }
    sql.push('DELETE FROM', b, 'WHERE');
    if (req.bucket.hasExtendedId) {
        sql.push('_id IN (SELECT _id FROM row_set WHERE _id IS NOT NULL) OR',
            '_idx IN (SELECT _idx FROM row_set WHERE _idx IS NOT NULL)');
    } else {
        /*
         * If this bucket does not have the extended ID column, use a basic
         * subquery to select rows to delete.
         */
        sql.push('_id IN (SELECT _id FROM', b, req.where.clause + ')');
    }
    sql = sql.join(' ');

    log.debug({
        bucket: req.bucket.name,
        sql: sql
    }, 'drop: entered');

    q = pg.query(sql, req.where.args);

    q.once('error', function (err) {
        log.debug(err, 'drop: failed');
        cb(err);
    });

    q.once('end', function (res) {
        log.debug({
            id: req._id,
            res: res
        }, 'drop: done');
        req._count = res.rowCount;
        cb();
    });
}


function deleteMany(options) {
    control.assertOptions(options);

    function _deleteMany(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var bucket = argv[0];
        var filter = argv[1];
        var opts = argv[2];

        var req = control.buildReq(opts, rpc, options);
        req.bucket = {
            name: bucket
        };
        req.rawFilter = filter;

        dtrace['delmany-start'].fire(function () {
            return ([req.msgid, req.req_id, bucket, filter]);
        });
        req.log.debug({
            bucket: bucket,
            filter: filter,
            opts: opts
        }, 'deleteMany: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: common.stdOutput(req)
        });
    }

    return (_deleteMany);
}


// --- Exports

module.exports = {
    deleteMany: deleteMany,
    pipeline: SUBPIPELINE
};
