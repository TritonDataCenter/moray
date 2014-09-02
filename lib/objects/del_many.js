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
    drop
];



///--- Handlers

function drop(req, cb) {
    cb = once(cb);

    var b = req.bucket.name;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = sprintf('DELETE FROM %s' +
                      '    WHERE _id IN (' +
                      '        SELECT _id FROM %s %s' +
                      '    )', b, b, req.where.clause);

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
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _deleteMany(b, f, opts, res) {
        var filter;
        var id = opts.req_id || libuuid.create();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            filter: filter,
            opts: opts
        }, 'deleteMany: entered');

        dtrace['delmany-start'].fire(function () {
            return ([res.msgid, id, b, f]);
        });

        try {
            filter = ldap.parseFilter(f);
        } catch (e) {
            log.debug(e, 'deleteMany: bad search filter');
            e = new InvalidQueryError(e, f);
            res.end(e);
            dtrace['delmany-done'].fire(function () {
                return ([res.msgid]);
            });
            return;
        }

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'deleteMany: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'deleteMany: transaction started');

            vasync.pipeline({
                funcs: PIPELINE,
                arg: {
                    bucket: {
                        name: b
                    },
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
                name: 'delmany',
                pg: pg,
                res: res
            }));
        });
    }

    return (_deleteMany);
}



///--- Exports

module.exports = {
    deleteMany: deleteMany,
    pipeline: PIPELINE
};
