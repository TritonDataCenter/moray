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


function checkEtag(req, cb) {
        var etag = req.etag || req._etag;
        if (!req.previous || !etag)
                return (cb());

        if (req.previous._etag !== etag)
                return (cb(new EtagConflictError(req.bucket.name,
                                                 req.key,
                                                 etag,
                                                 req.previous._etag)));

        return (cb());
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
        var ckey = cacheKey(opts.bucket.name);
        var log = opts.log;
        if ((b = BCACHE.get(ckey))) {
                opts.bucket = b;
                log.debug({
                        bucket: b
                }, 'loadBucket: done (cached)');
                return (cb(null));
        }

        b = opts.bucket.name;
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
                        opts.bucket = {
                                name: r.name,
                                index: JSON.parse(r.index),
                                pre: JSON.parse(r.pre).map(parseFunctor),
                                post: JSON.parse(r.post).map(parseFunctor),
                                mtime: new Date(r.mtime)
                        };

                        BCACHE.set(ckey, opts.bucket);
                        log.debug({
                                bucket: opts.bucket
                        }, 'loadBucket: done');
                        cb(null);
                } else {
                        cb(new BucketNotFoundError(opts.bucket.name));
                }
        });
        return (undefined);
}


function pgError(e) {
        var err;
        var msg;

        switch (e.code) {
        case '23505':
                /* JSSTYLED */
                msg = /.*violates unique constraint.*/.test(e.message);
                if (msg) {
                        // Key (str_u)=(hi) already exists
                        /* JSSTYLED */
                        msg = /.*\((.*)\)=\((.*)\)/.exec(e.detail);
                        err = new UniqueAttributeError(err, msg[1], msg[2]);
                } else {
                        err = e;
                }
                break;
        case '42601':
        case '42701':
                err = new InternalError(e, 'Invalid SQL: %s', e.message);
                break;
        default:
                err = e;
                break;
        }

        return (err);
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
                id: opts.value ? opts.value._id : (opts._id || -1),
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
        checkEtag: checkEtag,
        commit: commit,
        loadBucket: loadBucket,
        pgError: pgError,
        rowToObject: rowToObject,
        runPostChain: runPostChain,
        selectForUpdate: selectForUpdate
};