// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var vasync = require('vasync');

require('../errors');



///--- Globals

var sprintf = util.format;

var INDEX_TYPES = {
        string: true,
        number: true,
        'boolean': true
};

// Postgres rules:
// start with a letter, everything else is alphum or '_', and must be
// <= 63 characters in length
var BUCKET_NAME_RE = /^[a-zA-Z]\w{0,62}$/;

var RESERVED_BUCKETS = ['moray', 'search'];


///--- API

function commit(opts, cb) {
        opts.pgPool.commit(opts.pg, cb);
}


function buildIndexString(schema) {
        assert.object(schema, 'schema');

        var str = '';
        Object.keys(schema).forEach(function (k) {
                str += ',\n        ' + k + ' ' + typeToPg(schema[k].type);
                if (schema[k].unique)
                        str += ' UNIQUE';
        });

        return (str);
}


function createIndexes(opts, cb) {
        var bucket = opts.bucket;
        var indexes = opts.indexes;
        var log = opts.log;
        var pg = opts.pg;
        var queries = opts.indexes.map(function (k) {
                var sql = sprintf('CREATE %s INDEX %s_%s_idx ' +
                                  'ON %s(%s) ' +
                                  'WHERE %s IS NOT NULL',
                                  (opts.unique ? 'UNIQUE' : ''),
                                  bucket, k, bucket, k, k);
                return (sql);
        });

        log.debug({bucket: bucket}, 'createIndexes: entered');
        vasync.forEachParallel({
                func: function createIndex(sql, cb) {
                        log.debug('createIndexes: running %s', sql);
                        pg.query(sql, function (err) {
                                if (err) {
                                        log.warn({
                                                err: err,
                                                sql: sql
                                        }, 'createIndex: failed');
                                }
                                cb();
                        });
                },
                inputs: queries
        }, cb);
}


function typeToPg(type) {
        assert.string(type, 'type');

        var pgType;

        switch (type) {
        case 'number':
                pgType = 'NUMERIC';
                break;
        case 'boolean':
                pgType = 'BOOLEAN';
                break;
        case 'string':
                pgType = 'TEXT';
                break;
        default:
                throw new InvalidIndexTypeError(type);
        }

        return (pgType);
}


function validateIndexes(schema) {
        var i, j, k, k2, keys, msg, sub, subKeys;

        keys = Object.keys(schema);
        for (i = 0; i < keys.length; i++) {
                k = keys[i];
                if (typeof (k) !== 'string')
                        throw new InvalidIndexError('keys must be strings');
                if (typeof (schema[k]) !== 'object')
                        throw new InvalidIndexError('values must be objects');

                sub = schema[k];
                subKeys = Object.keys(sub);
                for (j = 0; j < subKeys.length; j++) {
                        k2 = subKeys[j];
                        switch (k2) {
                        case 'type':
                                if (sub[k2] !== 'string' &&
                                    sub[k2] !== 'number' &&
                                    sub[k2] !== 'boolean') {
                                        msg = k + '.type is invalid';
                                        throw new InvalidIndexError(msg);
                                }
                                break;
                        case 'unique':
                                if (typeof (sub[k2]) !== 'boolean') {
                                        msg = k + '.unique must be boolean';
                                        throw new InvalidIndexError(msg);
                                }
                                break;
                        default:
                                msg = k + '.' + k2 + ' is invalid';
                                throw new InvalidIndexError(msg);
                        }
                }
        }
}


function validateBucket(req, cb) {
        var bucket = req.bucket;
        var fn;
        var log = req.log;
        var post = [];
        var pre = [];

        log.debug('validate: entered (%j)', bucket);

        bucket.index = bucket.index || {};
        bucket.post = bucket.post || [];
        bucket.pre = bucket.pre || [];

        bucket.post = bucket.post.map(function (p) {
                return (eval('fn = ' + p));
        });

        bucket.pre = bucket.pre.map(function (p) {
                return (eval('fn = ' + p));
        });

        // Make JSLint shutup
        if (fn)
                fn = null;
        // End Make JSLint shutup

        if (!BUCKET_NAME_RE.test(bucket.name))
                return (cb(new InvalidBucketNameError(bucket.name)));

        if (RESERVED_BUCKETS.indexOf(bucket.name) !== -1)
                return (cb(new InvalidBucketNameError(bucket.name)));

        if (typeof (bucket.index) !== 'object' ||
            Array.isArray(bucket.index)) {
                return (cb(new InvalidIndexError('index is invalid')));
        }

        if (!Array.isArray(bucket.post))
                return (cb(new NotFunctionError('post')));

        try {
                assert.arrayOfFunc(bucket.post);
        } catch (e) {
                log.debug(e, 'validation of post failed');
                return (cb(new NotFunctionError('post')));
        }

        if (!Array.isArray(bucket.pre))
                return (cb(new NotFunctionError('pre')));

        try {
                assert.arrayOfFunc(bucket.pre);
        } catch (e) {
                log.debug(e, 'validation of pre failed');
                return (cb(new NotFunctionError('pre')));
        }

        try {
                validateIndexes(bucket.index);
        } catch (e) {
                return (cb(e));
        }

        log.debug('validate: done');
        return (cb());
}



///--- Exports

module.exports = {
        INDEX_TYPES: INDEX_TYPES,
        BUCKET_NAME_RE: BUCKET_NAME_RE,
        RESERVED_BUCKETS: RESERVED_BUCKETS,
        buildIndexString: buildIndexString,
        commit: commit,
        createIndexes: createIndexes,
        typeToPg: typeToPg,
        validateBucket: validateBucket
};
