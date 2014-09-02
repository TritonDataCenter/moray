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
var microtime = require('microtime');
var crc = require('crc');
var ldap = require('ldapjs');
var libuuid = require('libuuid');
var once = require('once');
var vasync = require('vasync');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;

// This is exported so that batch put can leverage it
var PIPELINE = [
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

    sql = sprintf('UPDATE %s' +
                  '    SET %s WHERE _id IN (' +
                  '        SELECT _id FROM %s %s' +
                  '    )', b.name, column, b.name, req.where.clause);

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
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _update(b, fields, f, opts, res) {
        var filter;
        var id = opts.req_id || libuuid.create();
        var keys = Object.keys(fields);
        var log = options.log.child({
            req_id: id
        });
        try {
            filter = ldap.parseFilter(f);
        } catch (e) {
            log.debug(e, 'bad search filter');
            e = new InvalidQueryError(e, f);
            res.end(e);
            return;
        }

        log.debug({
            bucket: b,
            fields: fields,
            filter: filter,
            opts: opts
        }, 'update: entered');

        if (keys.length === 0) {
            res.end(new FieldUpdateError(fields));
            return;
        }

        dtrace['update-start'].fire(function () {
            return ([res.msgid, id, b, fields, f]);
        });

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'update: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'update: transaction started');

            vasync.pipeline({
                funcs: PIPELINE,
                arg: {
                    bucket: {
                        name: b
                    },
                    fields: fields,
                    fieldKeys: keys,
                    filter: filter,
                    log: log,
                    pg: pg,
                    opts: opts,
                    manatee: manatee,
                    req_id: id,
                    res: res
                }
            }, common.pipelineCallback({
                log: log,
                name: 'update',
                pg: pg,
                res: res
            }));
        });
    }

    return (_update);
}



///--- Exports

module.exports = {
    update: update,
    pipeline: PIPELINE
};
