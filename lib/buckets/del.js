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

var common = require('./common');
var control = require('../control');
require('../errors');



///--- Handlers

function validate(req, cb) {
    var bucket = req.bucket;
    var log = req.log;

    log.debug('validate: entered (%j)', bucket);
    if (common.RESERVED_BUCKETS.indexOf(bucket.name) !== -1) {
        cb(new InvalidBucketNameError(bucket));
        return;
    }

    log.debug('validate: done');
    cb();
}


function checkExists(req, cb) {
    var log = req.log;

    var sql = 'SELECT * FROM buckets_config WHERE name = $1 FOR UPDATE';
    log.debug({
        bucket: req.bucket.name
    }, 'checkExists: entered');

    var q = req.pg.query(sql, [req.bucket.name]);
    var found = false;

    q.once('error', function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'checkExists: failed');
        cb(err);
    });

    q.once('row', function (r) {
        found = true;
    });

    q.once('end', function () {
        log.debug({
            found: found
        }, 'checkExists: done');
        if (found) {
            cb(null);
        } else {
            cb(new BucketNotFoundError(req.bucket.name));
        }
    });
}


function deleteConfig(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format('DELETE FROM buckets_config WHERE name = \'%s\'',
                          bucket.name);

    cb = once(cb);

    log.debug({ bucket: bucket.name }, 'deleteConfig: entered');

    q = pg.query(sql);
    q.once('error', cb);
    q.once('end', function () {
        log.debug({ bucket: bucket.name }, 'deleteConfig: done');
        cb();
    });
}


function dropTable(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format('DROP TABLE %s', bucket.name);

    cb = once(cb);

    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'dropTable: entered');

    q = pg.query(sql);
    q.once('error', cb);
    q.once('end', function () {
        log.debug({
            bucket: bucket.name
        }, 'dropTable: done');
        cb();
    });
}


function dropSequence(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format('DROP SEQUENCE IF EXISTS %s_serial', bucket.name);

    cb = once(cb);

    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'dropSequence: entered');

    q = pg.query(sql);

    q.once('error', cb);
    q.once('end', function () {
        log.debug('dropSequence: done');
        cb();
    });
}


function dropLockingSerial(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format('DROP TABLE IF EXISTS %s_locking_serial',
                      bucket.name);

    cb = once(cb);

    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'dropLockingSequence: entered');

    q = pg.query(sql);

    q.once('error', cb);
    q.once('end', function () {
        log.debug('dropLockingSequence: done');
        cb();
    });
}


function del(options) {
    control.assertOptions(options);
    var route = 'delBucket';

    function _del(bucket, opts, res) {
        var req = control.buildReq(opts, res, options.log);
        req.bucket = {
            name: bucket
        };

        req.log.debug({
            bucket: bucket,
            opts: opts
        }, '%s: entered', route);

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee, {}, true),
                validate,
                checkExists,
                deleteConfig,
                dropTable,
                dropSequence,
                dropLockingSerial
            ]
        });
    }

    return (_del);
}



///--- Exports

module.exports = {
    del: del
};
