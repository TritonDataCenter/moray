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
var libuuid = require('libuuid');

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');

var mod_errors = require('../errors');
var NotIndexedError = mod_errors.NotIndexedError;
var FieldUpdateError = mod_errors.FieldUpdateError;


///--- Globals

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
    common.decorateFilter,
    common.buildWhereClause,
    updateRows
];

// This is exported for batch 'update' operations
var SUBPIPELINE = PIPELINE.slice(1);

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


///--- Exports

module.exports = {
    update: update,
    pipeline: SUBPIPELINE
};
