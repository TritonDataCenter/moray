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
        var sql = sprintf('SELECT * FROM buckets_config WHERE name=\'%s\'',
                          req.bucket.name);

        log.debug({
                bucket: req.bucket.name
        }, 'loadBucket: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug({
                                bucket: req.bucket.name,
                                err: err
                        }, 'loadBucket: failed');
                        cb(err);
                } else {
                        log.debug({
                                bucket: result.rows[0]
                        }, 'loadBucket: done');
                        cb(null, result.rows[0]);
                }
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
                        opts: opts,
                        bucket: bucket
                }, 'getBucket: entered');

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'getBucket: no DB handle');
                                return (res.end(startErr));
                        }

                        log.debug({
                                pg: pg
                        }, 'getBucket: transaction started');

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
                                        res.end(b);
                                }

                                // We rollback either way
                                pg.rollback(function () {});
                        });

                        return (undefined);
                });
        }

        return (_get);
}



///--- Exports

module.exports = {
        get: get
};
