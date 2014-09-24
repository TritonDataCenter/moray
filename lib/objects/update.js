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
var libuuid = require('libuuid');

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
    updateRows
];


///--- Handlers

function updateRows(req, cb) {
    var b = req.bucket;
    var column  = '';
    var etag = 'u' + libuuid.create().substr(0, 7);
    var ok = true;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;
    var vals = req.where.args;

    vals.push(etag + '');
    column += '_etag=$' + vals.length;
    vals.push(Date.now());
    column += ', _mtime=$' + vals.length;

    cb = once(cb);

    req.fieldKeys.forEach(function (k) {
        if (!req.bucket.index[k]) {
            ok = false;
            return;
        }

        switch (req.bucket.index[k].type) {
        case 'boolean':
            vals.push(req.fields[k].toUpperCase());
            break;
        case 'number':
            vals.push(parseInt(req.fields[k], 10));
            break;
        case 'string':
            vals.push(req.fields[k] + '');
            break;
        default:
            break;
        }
        column += ',' + k + '=$' + vals.length;
    });

    if (!ok) {
        cb(new NotIndexedError(b.name, JSON.stringify(req.fields)));
        return;
    }

    sql = util.format(('UPDATE %s ' +
                       'SET %s WHERE _id IN ' +
                       '(SELECT _id FROM %s %s)'),
                      b.name, column, b.name, req.where.clause);

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
        req.res._etag = etag;
        req.res._count = res.rowCount;
        log.debug({
            count: req.res._count,
            etag: req.res._etag
        }, 'updateRows: done');
        cb();
    });
}


function update(options) {
    control.assertOptions(options);
    var route = 'updateObjects';

    function _update(bucket, fields, filter, opts, res) {
        var req = control.buildReq(opts, res, options.log);
        req.bucket = {
            name: bucket
        };
        req.fields = fields;
        req.fieldKeys = Object.keys(fields);
        if (req.fieldKeys.length === 0) {
            res.end(new FieldUpdateError(fields));
            return;
        }
        req.rawFilter = filter;

        dtrace['update-start'].fire(function () {
            return ([res.msgid, req.req_id, bucket, fields, filter]);
        });
        req.log.debug({
            bucket: bucket,
            fields: fields,
            filter: filter,
            opts: opts
        }, '%s: entered', route);

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee)
            ].concat(PIPELINE),
            cbOutput: common.stdOutput(req)
        });
    }

    return (_update);
}


///--- Exports

module.exports = {
    update: update,
    pipeline: PIPELINE
};
