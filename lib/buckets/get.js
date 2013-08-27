// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var uuid = require('node-uuid');
var vasync = require('vasync');

require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers

function loadBucket(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    var sql = sprintf('SELECT * FROM buckets_config WHERE name=\'%s\'',
                      req.bucket.name);

    log.debug({
        bucket: req.bucket.name
    }, 'loadBucket: entered');

    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'loadBucket: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        log.debug({
            bucket: row
        }, 'loadBucket: done');
        cb(null, row);
    });
}


function get(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _get(opts, bucket, res) {
        var log = options.log.child({
            req_id: opts.req_id || uuid.v1()
        });

        log.debug({
            bucket: bucket,
            opts: opts
        }, 'getBucket: entered');

        manatee.pg({read: true}, function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'getBucket: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'getBucket: client acquired');

            vasync.pipeline({
                funcs: [ loadBucket ],
                arg: {
                    bucket: {
                        name: bucket
                    },
                    log: log,
                    pg: pg,
                    manatee: manatee
                }
            }, function (err, results) {
                if (err) {
                    log.warn(err, 'getBucket: failed');
                    res.end(err);
                } else if (!results.successes[0]) {
                    log.debug('getBucket: not found');
                    err = new BucketNotFoundError(bucket);
                    res.end(err);
                } else {
                    var b = results.successes.pop();
                    log.debug(b, 'getBucket: done');
                    b.options = b.options || {};
                    b.options.version =
                        b.options.version || 0;
                    res.end(b);
                }
                pg.release();
            });
        });
    }

    return (_get);
}



///--- Exports

module.exports = {
    get: get
};
