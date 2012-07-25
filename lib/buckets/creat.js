// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var uuid = require('node-uuid');
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



///--- Internal Functions

// function diffSchema(prev, next) {
//         assert.object(prev, 'previous');
//         assert.object(next, 'next');

//         var diff = {
//                 add: [],
//                 del: [],
//                 mod: []
//         };

//         Object.keys(next).forEach(function (k) {
//                 if (prev[k] === undefined) {
//                         diff.add.push(k);
//                 } else if (!deepEqual(next[k], prev[k])) {
//                         diff.mod.push(k);
//                 }
//         });

//         Object.keys(prev).forEach(function (k) {
//                 if (!next[k])
//                         diff.del.push(k);
//         });

//         return (diff);
// }


function indexString(schema) {
        assert.object(schema, 'schema');

        var str = '';
        Object.keys(schema).forEach(function (k) {
                str += ',\n        ' + k + ' ' + typeToPg(schema[k].type);
                if (schema[k].unique)
                        str += ' UNIQUE';
        });

        return (str);
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



///--- Handlers

function validate(req, cb) {
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


function insertConfig(req, cb) {
        if (req.prev)
                return (cb());

        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = 'INSERT INTO buckets_config (name, index, pre, post)' +
                ' VALUES ($1, $2, $3, $4)';
        var values = [
                bucket.name,
                JSON.stringify(bucket.index),
                JSON.stringify(bucket.pre),
                JSON.stringify(bucket.post)
        ];

        log.debug({
                bucket: bucket
        }, 'insertConfig: entered');
        pg.query(sql, values, function (err, result) {
                if (err) {
                        log.debug({
                                bucket: bucket.name,
                                err: err
                        }, 'insertConfig: failed');
                        return (cb(err));
                }

                log.debug({
                        bucket: bucket.name,
                }, 'insertConfig: done');
                return (cb());
        });
}


function createTable(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('CREATE TABLE %s (' +
                          '_id SERIAL, ' +
                          '_key TEXT PRIMARY KEY, ' +
                          '_value TEXT NOT NULL, ' +
                          '_etag CHAR(8) NOT NULL, ' +
                          '_mtime BIGINT NOT NULL%s)',
                          bucket.name,
                          indexString(bucket.index))

        log.debug({
                bucket: bucket.name,
                sql: sql
        }, 'createTable: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'createTable: failed');
                        cb(err);
                } else {
                        log.debug('createTable: done');
                        cb();
                }
        });
}


function createIndexes(req, cb) {
        // we can skip unique indexes, as those implicitly have a PG index
        // at create table time

        var bucket = req.bucket;
        var log = req.log;
        var name = bucket.name;
        var pg = req.pg;
        var schema = bucket.index;
        var indexes = Object.keys(schema).filter(function (k) {
                return (!schema[k].unique);
        }).concat('_id', '_etag', '_mtime').map(function (k) {
                var sql = sprintf('CREATE INDEX %s_%s_idx ' +
                                  'ON %s(%s) ' +
                                  'WHERE %s IS NOT NULL',
                                  name, k, name, k, k);
                return (sql);
        });

        log.debug({bucket: bucket.name}, 'createIndexes: entered');
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
                inputs: indexes
        }, cb);
}


function commit(req, cb) {
        req.pgPool.commit(req.pg, cb);
}


function creat(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.pg, 'options.pg');

        var pg = options.pg;

        function _creat(name, cfg, opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        bucket: name,
                        cfg: cfg,
                        opts: opts
                }, 'createBucket: entered');

                pg.start(function (err, client) {
                        log.debug({
                                pg: client
                        }, 'createBucket: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        validate,
                                        insertConfig,
                                        createTable,
                                        createIndexes,
                                        commit
                                ],
                                arg: {
                                        bucket: {
                                                name: name,
                                                index: cfg.index || {},
                                                pre: cfg.pre || [],
                                                post: cfg.post || []
                                        },
                                        log: log,
                                        pg: client,
                                        pgPool: pg
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'createBucket: failed');
                                        pg.rollback(client, function () {
                                                res.end(err);
                                        });
                                } else {
                                        log.debug('createBucket: done');
                                        res.end();
                                }
                        });
                });
        }

        return (_creat);
}



///--- Exports

module.exports = {
        creat: creat
};
