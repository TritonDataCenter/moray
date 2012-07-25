// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var uuid = require('node-uuid');
var vasync = require('vasync');

require('../errors');



///--- Globals

var sprintf = util.format;



///--- Internal Functions

function rowsToBucket(rows) {
        if (!rows || rows.length === 0)
                return (null);

        var b = rows.pop();
        var fn;

        b.index = JSON.parse(b.index);
        b.post = JSON.parse(b.post);
        b.pre = JSON.parse(b.pre);

        b.post = b.post.map(function (p) {
                return (eval('fn = ' + p));
        });
        b.pre = b.pre.map(function (p) {
                return (eval('fn = ' + p));
        });
        // Make JSLint shutup
        if (fn)
                fn = null;
        // End Make JSLint shutup

        return (b);
}


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
                        var bucket = rowsToBucket(result.rows);
                        log.debug({
                                bucket: bucket
                        }, 'loadBucket: done');
                        cb(null, bucket);
                }
        });
}


function get(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.pg, 'options.pg');

        var pg = options.pg;

        function _get(opts, bucket, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        opts: opts,
                        bucket: bucket
                }, 'buckets.get: entered');

                pg.start(function (err, client) {
                        log.debug({
                                pg: client
                        }, 'buckets.get: transaction started');

                        vasync.pipeline({
                                funcs: [ loadBucket ],
                                arg: {
                                        bucket: {
                                                name: bucket
                                        },
                                        log: log,
                                        pg: client,
                                        pgPool: pg
                                }
                        }, function (err, results) {
                                if (err) {
                                        log.warn(err, 'get: failed');
                                        res.end(err);
                                } else if (!results.successes[0]) {
                                        log.debug('get: bucket not found');
                                        err = new BucketNotFoundError(bucket);
                                        res.end(err);
                                } else {
                                        var b = results.successes.pop();
                                        log.debug('get: done');
                                        res.end(b);
                                }

                                // We rollback either way
                                pg.rollback(client, function () {});
                        });
                });
        }

        return (_get);
}



///--- Exports

module.exports = {
        get: get
};
