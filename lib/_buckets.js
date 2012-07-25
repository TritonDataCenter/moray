// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var restify = require('restify');
var vasync = require('vasync');

var common = require('./common');
require('./errors');



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




///--- Helpers

function diffSchema(prev, next) {
        assert.object(prev, 'previous');
        assert.object(next, 'next');

        var diff = {
                add: [],
                del: [],
                mod: []
        };

        Object.keys(next).forEach(function (k) {
                if (prev[k] === undefined) {
                        diff.add.push(k);
                } else if (!deepEqual(next[k], prev[k])) {
                        diff.mod.push(k);
                }
        });

        Object.keys(prev).forEach(function (k) {
                if (!next[k])
                        diff.del.push(k);
        });

        return (diff);
}


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
                        return (new InvalidIndexError('keys must be strings'));
                if (typeof (schema[k]) !== 'object')
                        return (new InvalidIndexError('values must be objects'));

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
                                        return (new InvalidIndexError(msg));
                                }
                                break;
                        case 'unique':
                                if (typeof (sub[k2]) !== 'boolean') {
                                        msg = k + '.unique must be boolean';
                                        return (new InvalidIndexError(msg));
                                }
                                break;
                        default:
                                msg = k + '.' + k2 + ' is invalid';
                                return (new InvalidIndexError(msg));
                        }
                }
        }

        return (null);
}



///--- Handlers

//-- PUT Handlers

function validatePutBucketRequest(req, cb) {
        var post = [];
        var pre = [];

        req.log.debug('validatePutBucketRequest: entered');

        req.params.index = req.params.index || {};
        req.params.post = req.params.post || [];
        req.params.pre = req.params.pre || [];

        req.params.post = req.params.post.map(function (p) {
                return (eval('fn = ' + p));
        });

        req.params.pre = req.params.pre.map(function (p) {
                return (eval('fn = ' + p));
        });

        // Make JSLint shutup
        if (fn)
                fn = null;
        // End Make JSLint shutup

        if (!BUCKET_NAME_RE.test(req.params.bucket))
                return (next(new InvalidBucketNameError(req.params.bucket)));

        if (RESERVED_BUCKETS.indexOf(req.params.bucket) !== -1)
                return (next(new InvalidBucketNameError(bucket)));

        if (typeof (req.params.index) !== 'object' ||
            Array.isArray(req.params.index)) {
                return (next(new InvalidIndexError('index is invalid')));
        }

        if (!Array.isArray(req.params.post))
                return (next(new NotFunctionError('post')));

        try {
                assert.arrayOfFunc(req.params.post);
        } catch (e) {
                req.log.debug(e, 'validation of req.params.post failed');
                return (next(new NotFunctionError('post')));
        }

        if (!Array.isArray(req.params.pre))
                return (next(new NotFunctionError('pre')));

        try {
                assert.arrayOfFunc(req.params.pre);
        } catch (e) {
                req.log.debug(e, 'validation of req.params.pre failed');
                return (next(new NotFunctionError('pre')));
        }

        req.log.debug('validatePutBucketRequest: checks ok; inspecting index');
        return (next(validateIndexes(req.params.index)));
}


function insertBucketConfig(req, res, next) {
        if (req.bucket)
                return (next());

        var log = req.log;
        var opts = {
                client: req.pg,
                sql: 'INSERT INTO buckets_config (name, index, pre, post)' +
                        ' VALUES ($1, $2, $3, $4)',
                values: [
                        req.params.bucket,
                        JSON.stringify(req.params.index),
                        JSON.stringify(req.params.pre),
                        JSON.stringify(req.params.post)
                ]
        };

        log.debug({
                bucket: req.params.bucket,
                record: opts.values
        }, 'insertBucketConfig: entered');
        req.pgPool.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'insertBucketConfig: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
        return (undefined);
}


function updateBucketConfig(req, res, next) {
        if (!req.bucket)
                return (next());

        var log = req.log;
        var opts = {
                client: req.pg,
                sql: sprintf('UPDATE buckets_config SET index=\'%s\',' +
                             ' pre=$moray$%s$moray$,' +
                             'post=$moray$%s$moray$ WHERE name=\'%s\'',
                             JSON.stringify(req.params.index),
                             JSON.stringify(req.params.pre),
                             JSON.stringify(req.params.post),
                             req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket
        }, 'updateBucketConfig: entered');
        req.pgPool.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'updateBucketConfig: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
        return (undefined);
}


function alterEntryTable(req, res, next) {
        if (!req.bucket)
                return (next());

        var bucket = req.params.bucket;
        var log = req.log;
        var prev = req.bucket.index;
        var index = req.params.index;
        var diff = diffSchema(prev, index);
        var tasks;
        if (diff.mod.length > 0)
                return (next(new SchemaChangeError(bucket, diff.mod.join())));

        log.debug({diff: diff}, 'alterEntryTable: entered');

        // Build up a vasync series of DROP then ADD columns
        tasks = diff.del.map(function (column) {
                var sql = sprintf('ALTER TABLE %s_entry DROP COLUMN %s',
                                  bucket, column);
                return (sql);
        }).concat(diff.add.map(function (column) {
                var type = typeToPg(index[column].type);
                var sql = sprintf('ALTER TABLE %s_entry ADD COLUMN %s %s',
                                  bucket, column, type);
                if (index[column].unique)
                        sql += ' UNIQUE';
                return (sql);
        }));

        log.debug('alterEntryTable: issuing SQL commands');
        vasync.forEachParallel({
                func: function update(sql, cb) {
                        req.pgPool.query({
                                client: req.pg,
                                sql: sql
                        }, cb);
                },
                inputs: tasks
        }, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: bucket,
                                err: err
                        }, 'alterEntryTable: %s', (err ? 'failed' : 'done'));
                }
                req.diff = diff;
                next(err);
        });
        return (undefined);
}


function createEntryTable(req, res, next) {
        if (req.bucket)
                return (next());

        var log = req.log;
        var opts = {
                client: req.pg,
                sql: sprintf('CREATE TABLE %s_entry (' +
                             '_id SERIAL, ' +
                             '_key TEXT PRIMARY KEY, ' +
                             '_value TEXT NOT NULL, ' +
                             '_etag CHAR(32) NOT NULL, ' +
                             '_mtime TIMESTAMP NOT NULL ' +
                             'DEFAULT CURRENT_TIMESTAMP%s)',
                             req.params.bucket,
                             indexString(req.params.index))
        };

        log.debug({
                bucket: req.params.bucket
        }, 'createEntryTable: entered');
        req.pgPool.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'createEntryTable: %s', (err ? 'failed' : 'done'));
                }
                // Stub out the diff so createIndexes will work correctly
                req.diff = {
                        add: Object.keys(req.params.index)
                };
                next(err);
        });
        return (undefined);
}


function createIndexes(req, res, next) {
        // We just blindly stomp over old indexes. Postgres will throw,
        // and we ignore

        var bucket = req.params.bucket;
        var schema = req.params.index;
        var sql = req.diff.add.filter(function (k) {
                return (!schema[k].unique);
        }).map(function (k) {
                var sql = sprintf('CREATE INDEX %s_entry_%s_idx ' +
                                  'ON %s_entry(%s) ' +
                                  'WHERE %s IS NOT NULL',
                                  bucket, k, bucket, k, k);
                return (sql);
        });

        vasync.forEachParallel({
                func: function createIndex(sql, cb) {
                        var opts = {
                                client: req.pg,
                                sql: sql
                        };
                        req.pgPool.query(opts, function (err) {
                                if (err) {
                                        req.log.warn({
                                                err: err,
                                                sql: sql
                                        }, 'createIndex: failed');
                                }
                                cb();
                        });
                },
                inputs: sql
        }, next);
}


function putDone(req, res, next) {
        res.send(204);
        next();
}


//-- GET Handlers

function getDone(req, res, next) {
        if (req.bucket) {
                res.send(200, req.bucket);
                next();
        } else {
                next(new BucketNotFoundError(req.params.bucket));
        }
}

//-- DELETE Handlers

function dropTable(req, res, next) {
        var log = req.log;
        var opts = {
                client: req.pg,
                sql: sprintf('DROP TABLE %s_entry', req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket,
        }, 'dropTable: entered');
        req.pgPool.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'dropTable: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
}


function deleteBucketConfig(req, res, next) {
        var log = req.log;
        var opts = {
                client: req.pg,
                sql: sprintf('DELETE FROM buckets_config WHERE name = \'%s\'',
                             req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket,
        }, 'deleteBucketConfig: entered');
        req.pgPool.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err

                        }, 'deleteBucketConfig: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
}


function delDone(req, res, next) {
        res.send(204);
        next();
}



///--- Exports

module.exports = {
        put: function put() {
                var chain = [
                        validatePutBucketRequest,
                        common.startTransaction,
                        common.loadBucket,
                        insertBucketConfig,
                        updateBucketConfig,
                        createEntryTable,
                        alterEntryTable,
                        createIndexes,
                        common.commitTransaction,
                        putDone
                ];
                return (chain);
        },

        get: function get() {
                var chain = [
                        common.startTransaction,
                        common.loadBucket,
                        common.rollbackTransaction,
                        getDone
                ];
                return (chain);
        },

        del: function del() {
                var chain = [
                        common.startTransaction,
                        common.loadBucket,
                        dropTable,
                        deleteBucketConfig,
                        common.commitTransaction,
                        delDone
                ];
                return (chain);
        }
};
