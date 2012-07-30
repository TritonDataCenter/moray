// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var crc = require('crc');
var Cache = require('expiring-lru-cache');
var vasync = require('vasync');

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


function commit(req, cb) {
        req.pg.commit(cb);
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
                        function parseFunctor(f) {
                                var fn;
                                assert.ok(eval('fn = ' + f));
                                return (fn);
                        }

                        var r = result.rows[0];
                        b = {
                                name: r.name,
                                index: JSON.parse(r.index),
                                pre: JSON.parse(r.pre).map(parseFunctor),
                                post: JSON.parse(r.post).map(parseFunctor),
                                mtime: new Date(r.mtime)
                        };

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


function rowToObject(bucket, row) {
        assert.string(bucket, 'bucket');
        assert.object(row, 'row');

        return ({
                bucket: bucket,
                key: row._key,
                value: JSON.parse(row._value),
                _id: row._id,
                _etag: row._etag,
                _mtime: row._mtime
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

        return (undefined);
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