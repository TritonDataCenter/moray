// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var crc = require('crc');
var Cache = require('expiring-lru-cache');

require('../errors');



///--- Globals

var sprintf = util.format;

var BCACHE = new Cache({
        name: 'BucketCache',
        size: 100,
        expiry: (300 * 1000)
});



///--- API

function cacheKey(b, k) {
        assert.string(b, 'bucket');

        var str = '/' + b;
        if (k)
                str += '/' + k;

        return (str);
}


function commit(opts, cb) {
        opts.pgPool.commit(opts.pg, cb);
}


function createEtag(opts, cb) {
        opts.etag = crc.hex32(crc.crc32(opts._value));
        cb();
}


function loadBucket(opts, cb) {
        assert.object(opts, 'options');
        assert.object(opts.bucket, 'options.bucket');
        assert.object(opts.log, 'options.log');
        assert.object(opts.pg, 'options.pg');
        assert.func(cb, 'callback');

        var b;
        if ((b = BCACHE.get(cacheKey(opts.bucket.name)))) {
                return (cb(null, b));
        }

        b = opts.bucket.name;
        var fn;
        var log = opts.log;
        var pg = opts.pg;
        var sql = sprintf('SELECT * FROM buckets_config WHERE name=\'%s\'', b);

        log.debug({
                bucket: b
        }, 'loadBucket: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug({
                                bucket: b,
                                err: err
                        }, 'loadBucket: failed');
                        cb(err);
                } else if (result.rows.length > 0) {
                        b = result.rows[0];
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

                        BCACHE.set(opts.bucket.name, b);
                        opts.bucket = b;
                        log.debug({
                                bucket: b
                        }, 'loadBucket: done');
                        cb(null);
                } else {
                        cb(new BucketNotFoundError(opts.bucket.name));
                }
        });
        return (undefined);
}


function rowToObject(bucket, key, row) {
        assert.string(bucket, 'bucket');
        assert.string(key, 'key');
        assert.object(row, 'row');

        return ({
                _bucket: bucket,
                _id: row._id,
                _etag: row._etag,
                _key: key,
                _mtime: new Date(row._mtime),
                value: JSON.parse(row._value)
        });
}


function runPostChain(opts, cb) {
        if (opts.bucket.post.length === 0)
                return (cb());

        var cookie = {
                bucket: opts.bucket.name,
                id: opts.value._id,
                key: opts.key,
                log: opts.log,
                pg: opts.pg,
                schema: opts.bucket.index,
                value: opts.value
        };
        var log = opts.log;

        log.debug('runPostChain: entered');

        vasync.pipeline({
                funcs: opts.bucket.post,
                arg: cookie
        }, function (err) {
                if (err) {
                        log.debug(err, 'runPostChain: fail');
                        cb(err);
                } else {
                        log.debug('runPostChain: done');
                        cb();
                }
        });
}


function selectForUpdate(opts, cb) {
        var bucket = opts.bucket.name;
        var key = opts.key;
        var log = opts.log;
        var pg = opts.pg;
        var sql = sprintf('SELECT * FROM %s WHERE _key=\'%s\' FOR UPDATE',
                          bucket, key);

        log.debug({
                bucket: bucket,
                key: key
        }, 'selectForUpdate: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'selectForUpdate: failed');
                        cb(err);
                } else {
                        if (result.rows.length > 0)
                                opts.previous = result.rows[0];

                        log.debug({
                                previous: opts.previous || null
                        }, 'selectForUpdate: done');
                        cb();
                }
        });
}



///--- Exports

module.exports = {
        cacheKey: cacheKey,
        commit: commit,
        loadBucket: loadBucket,
        rowToObject: rowToObject,
        runPostChain: runPostChain,
        selectForUpdate: selectForUpdate
};