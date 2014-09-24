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

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');


///--- Globals

// This is exported so that batch put can leverage it
var PIPELINE = [
    common.parseFilter,
    common.loadBucket,
    common.buildWhereClause,
    drop
];


///--- Handlers

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
        req.res._count = res.rowCount;
        cb();
    });
}


function deleteMany(options) {
    control.assertOptions(options);
    var route = 'deleteMany';

    function _deleteMany(bucket, filter, opts, res) {
        var req = control.buildReq(opts, res, options.log);
        req.bucket = {
            name: bucket
        };
        req.rawFilter = filter;

        dtrace['delmany-start'].fire(function () {
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
                control.getPGHandle(route, options.manatee, {}, true)
            ].concat(PIPELINE),
            cbOutput: common.stdOutput(req)
        });
    }

    return (_deleteMany);
}


///--- Exports

module.exports = {
    deleteMany: deleteMany,
    pipeline: PIPELINE
};
