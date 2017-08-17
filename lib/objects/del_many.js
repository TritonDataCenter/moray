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
    var sql = util.format(('DELETE FROM %s ' +
                           'WHERE _id IN (SELECT _id FROM %s %s)'),
                          b, b, req.where.clause);

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
