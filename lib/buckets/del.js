// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var libuuid = require('libuuid');
var once = require('once');
var vasync = require('vasync');

var common = require('./common');
require('../errors');



///--- Globals

var sprintf = util.format;



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


function deleteConfig(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = sprintf('DELETE FROM buckets_config WHERE name = \'%s\'',
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
    var sql = sprintf('DROP TABLE %s', bucket.name);

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
    var sql = sprintf('DROP SEQUENCE IF EXISTS %s_serial', bucket.name);

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
    var sql = sprintf('DROP TABLE IF EXISTS %s_locking_serial',
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
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _del(bucket, opts, res) {
        var log = options.log.child({
            req_id: opts.req_id || libuuid.create()
        });

        log.debug({
            bucket: bucket,
            opts: opts
        }, 'delBucket: entered');

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'delBucket: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'delBucket: transaction started');

            vasync.pipeline({
                funcs: [
                    validate,
                    deleteConfig,
                    dropTable,
                    //dropSequence,
                    dropLockingSerial
                ],
                arg: {
                    bucket: {
                        name: bucket
                    },
                    log: log,
                    pg: pg,
                    manatee: manatee
                }
            }, common.pipelineCallback({
                log: log,
                name: 'delBucket',
                pg: pg,
                res: res
            }));

        });
    }

    return (_del);
}



///--- Exports

module.exports = {
    del: del
};
