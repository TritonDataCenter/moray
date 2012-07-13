// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var async = require('async');
var Cache = require('expiring-lru-cache');
var deepEqual = require('deep-equal');

var errors = require('../errors');
var SQL = require('./sql');

require('../args');



///--- Globals

var sprintf = util.format;

// Postgres rules:
// start with a letter, everything else is alphum or '_', and must be
// <= 63 characters in length
var BUCKET_NAME_RE = /^[a-zA-Z]\w{0,62}$/;

var RESERVED_BUCKETS = ['moray', 'search'];


///--- Helpers

function typeToPg(type) {
    assertString('type', type);

    switch (type) {
    case 'number':
        return 'NUMERIC';
    case 'boolean':
        return 'BOOLEAN';
    case 'string':
        return 'TEXT';
    default:
        throw new InvalidIndexTypeError(type);
    }
}

function indexString(schema) {
    assertObject('schema', schema);

    var str = '';
    Object.keys(schema).forEach(function (k) {
        str += ',\n        ' + k + ' ' + typeToPg(schema[k].type);
        if (schema[k].unique)
            str += ' UNIQUE';
    });

    return str;
}


function diffSchema(prev, next) {
    assertObject('previous', prev);
    assertObject('next', next);

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

    return diff;
}


function validateSchema(schema) {
    if (typeof (schema) !== 'object' || Array.isArray(schema))
        throw new InvalidSchemaError('not an object');

    var keys = Object.keys(schema);
    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        if (typeof (k) !== 'string')
            throw new InvalidSchemaError('top-level keys must be strings');
        if (typeof (schema[k]) !== 'object')
            throw new InvalidSchemaError('each key must be an object');

        var sub = schema[k];
        var subKeys = Object.keys(sub);
        for (var j = 0; j < subKeys.length; j++) {
            var k2 = subKeys[j];
            switch (k2) {
            case 'type':
                if (sub[k2] !== 'string' &&
                    sub[k2] !== 'number' &&
                    sub[k2] !== 'boolean')
                    throw new InvalidSchemaError(k + '.type is invalid');
                break;
            case 'unique':
                if (typeof (sub[k2]) !== 'boolean')
                    throw new InvalidSchemaError(k + '.unique must be boolean');
                break;
            default:
                throw new InvalidSchemaError(k + '.' + k2 + ' is invalid');
            }
        }
    }
}



///--- API

/**
 * Constructs a new BucketManager.
 *
 * @arg {Object} options - configuration.
 * @arg {Object} options.log - bunyan logger
 * @arg {Object} options.pgClient - postgres client.
 * @constructor
 */
function BucketManager(options) {
    assertObject('options', options);
    assertObject('options.pgClient', options.pgClient);
    assertObject('options.log', options.log);

    EventEmitter.call(this);

    this.bucketCache = new Cache({
        name: 'BucketManagerCache',
        size: options.bucketCacheSize || 100,
        expiry: options.bucketCacheExpiry || 60
    });
    this.log = options.log;
    this.pgClient = options.pgClient;

    var self = this;
    this.init(function (err) {
        if (err)
            return self.emit('error', err);

        return self.list(function (err2, buckets) {
            if (err2)
                return self.emit('error', err);

            return self.emit('ready', buckets);
        });
    });
}
util.inherits(BucketManager, EventEmitter);


/**
 * Ensures the one table we really need is actually present in Postgres.
 *
 * @arg {Function} callback - function (err).
 */
BucketManager.prototype.init = function init(callback) {
    assertFunction('callback', callback);

    var exists = false;
    var jid = this.log.queryId();
    var log = this.log;
    var pg;
    var self = this;

    log.debug({job_id: jid}, 'BucketManager::init entered');

    return async.series([
        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function check(cb) {
            return pg.query(SQL.CHECK_CONFIG_EXISTS, function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length > 0)
                    exists = true;

                return cb();
            });
        },

        function createConfig(cb) {
            if (exists)
                return cb();

            return pg.query(SQL.CREATE_CONFIG, cb);
        },


        function createSequence(cb) {
            if (exists)
                return cb();

            return pg.query(SQL.CREATE_SEQUENCE, cb);
        },

        function commit(cb) {
            if (exists)
                return pg.rollback(cb);

            return pg.commit(cb);
        }
    ], function initCallback(err) {
        if (err) {
            if (pg) {
                return pg.rollback(function () {
                    return callback(err);
                });
            }
            return callback(err);
        }

        log.debug({
            job_id: jid
        }, 'BucketManager::init done');

        return callback(null);
    });
};


/**
 * Creates or updates a bucket in Postgres.
 *
 * If the bucket already exists, this will error out, as we don't currently
 * support this being able to do ALTER TABLE, etc. with new schema.
 *
 * options.schema looks like:
 *
 * {
 *     email: {
 *         type: 'string',
 *         unique: true
 *     },
 *     department: {
 *         type: 'number',
 *         unique: false
 *     },
 *     ismanager: {
 *         type: 'boolean',
 *          unique: false
 *     }
 * }
 *
 * @arg {String} bucket - bucket name.
 * @arg {Object} options - configuration object.
 * @arg {Object} options.schema - what indexes to manage.
 * @arg {Array} options.pre - (optional) list of preCommits to execute on put.
 * @arg {Array} options.post - (optional) list of postCommits to execute on put.
 * @arg {Function} callback - function (err).
 */
BucketManager.prototype.put = function put(bucket, options, callback) {
    assertString('bucket', bucket);
    if (typeof (options) === 'function') {
        callback = options;
        options = {};
    }
    assertObject('options', options);
    options.schema = options.schema || {};
    options.pre = options.pre || [];
    options.post = options.post || [];
    assertObject('options.schema', options.schema);
    assertArray('options.pre', 'function', options.pre);
    assertArray('options.post', 'function', options.post);
    assertFunction('callback', callback);

    if (!BUCKET_NAME_RE.test(bucket))
        throw new InvalidBucketNameError(bucket);

    if (RESERVED_BUCKETS.indexOf(bucket) !== -1)
        throw new ReservedBucketError(bucket);

    validateSchema(options.schema);

    var indexes = indexString(options.schema);
    var jid = this.log.queryId();
    var log = this.log;
    var old;
    var post = [];
    var pre = [];
    var pg;
    var schema;
    var self = this;

    log.debug({
        bucket: bucket,
        schema: options.schema,
        pre: options.pre,
        post: options.post,
        job_id: jid
    }, 'BucketManager::put entered');

    options.pre.forEach(function (p) {
        pre.push(p.toString());
    });

    options.post.forEach(function (p) {
        post.push(p.toString());
    });

    schema = JSON.stringify(options.schema);
    pre = JSON.stringify(pre);
    post = JSON.stringify(post);

    return async.series([
        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function checkIfExists(cb) {
            var sql = sprintf(SQL.GET_BUCKET_CONFIG, bucket);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length > 0)
                    old = rows[0];

                return cb();
            });
        },

        function insertBucketConfig(cb) {
            if (old)
                return cb();

            var values;
            try {
                values = [
                    bucket,
                    schema,
                    pre,
                    post
                ];
            } catch (e) {
                return cb(e);
            }
            return pg.query(SQL.INSERT_CONFIG, values, cb);
        },

        function updateBucketConfig(cb) {
            if (!old)
                return cb();

            var sql = sprintf(SQL.UPDATE_CONFIG, schema, pre, post, bucket);
            return pg.query(sql, cb);
        },

        function alterEntryTable(cb) {
            if (!old)
                return cb();

            var diff;
            var tasks = [];
            try {
                schema = JSON.parse(old.schema);
            } catch (e) {
                return cb(e);
            }

            diff = diffSchema(schema, options.schema);
            if (diff.mod.length > 0)
                return cb(new SchemaChangeError(bucket, diff.mod.join()));

            function dropIterator(k) {
                tasks.push(function (_cb) {
                    var sql = sprintf(SQL.DROP_ENTRY_COLUMN, bucket, k);
                    return pg.query(sql, _cb);
                });
                tasks.push(function (_cb) {
                    var sql = sprintf(SQL.DROP_TOMBSTONE_COLUMN, bucket, k);
                    return pg.query(sql, _cb);
                });
            }
            function addIterator(k) {
                var type = typeToPg(options.schema[k].type);

                tasks.push(function (_cb) {
                    var sql = sprintf(SQL.ADD_ENTRY_COLUMN, bucket, k, type);
                    if (options.schema[k].unique)
                        sql += ' UNIQUE';

                    return pg.query(sql, _cb);
                });
                if (!options.schema[k].unique) {
                    tasks.push(function (_cb) {
                        var sql = SQL.createEntryIndexString(bucket, k);
                        return pg.query(sql, _cb);
                    });
                }
                tasks.push(function (_cb) {
                    var sql = sprintf(SQL.ADD_TOMBSTONE_COLUMN,
                                      bucket, k, type);
                    return pg.query(sql, _cb);
                });
            }

            diff.del.forEach(dropIterator);
            diff.mod.forEach(dropIterator);
            diff.add.forEach(addIterator);
            diff.mod.forEach(addIterator);

            if (tasks.length === 0)
                return cb();

            return async.series(tasks, cb);
        },

        function createEntryTable(cb) {
            if (old)
                return cb();

            var sql = sprintf(SQL.CREATE_ENTRY_TABLE, bucket, indexes);
            return pg.query(sql, cb);
        },

        function createTombstoneTable(cb) {
            if (old)
                return cb();

            var ti = indexes.replace(/ UNIQUE/g, '');
            var sql = sprintf(SQL.CREATE_TOMBSTONE_TABLE, bucket, ti);
            return pg.query(sql, cb);
        },

        function createIndexes(cb) {
            if (old)
                return cb();

            var sql = [];
            Object.keys(options.schema).forEach(function (k) {
                if (options.schema[k].unique)
                    return;

                sql.push(function (_cb) {
                    var str = SQL.createEntryIndexString(bucket, k);
                    return pg.query(str, _cb);
                });
            });

            return async.series(sql, cb);
        },

        function commit(cb) {
            return pg.commit(cb);
        }
    ], function putCallback(err) {
        if (err) {
            if (pg) {
                return pg.rollback(function () {
                    return callback(err);
                });
            }
            return callback(err);
        }

        var _bucket = {
            schema: options.schema,
            pre: options.pre,
            post: options.post
        };

        log.debug({
            bucket: bucket,
            job_id: jid,
            _bucket: _bucket
        }, 'BucketManager::put done');

        self.bucketCache.set(bucket, _bucket);
        return callback(null, _bucket);
    });
};


/**
 * Lists all registered buckets.
 *
 * @arg {Function} callback - function (err, buckets).
 */
BucketManager.prototype.list = function list(callback) {
    assertFunction('callback', callback);

    var log = this.log;
    var jid = log.queryId();
    var self = this;

    log.debug({job_id: jid}, 'BucketManager::list entered');
    return this.pgClient.query(SQL.LIST_BUCKETS, function (err, rows) {
        if (err)
            return callback(err);

        var buckets = {};

        try {
            var fn;

            rows.forEach(function (r) {
                buckets[r.name] = {
                    schema: JSON.parse(r.schema)
                };

                buckets[r.name].pre = JSON.parse(r.pre).map(function (p) {
                    return eval('fn=' + p);
                });

                buckets[r.name].post = JSON.parse(r.post).map(function (p) {
                    return eval('fn=' + p);
                });
            });

            // javascriptlint whinage
            if (fn) {
                fn = null;
            }
        } catch (e) {
            log.fatal({err: e, data: rows}, 'Invalid JSON in postgres');
            return callback(e);
        }

        log.debug({
            buckets: buckets,
            job_id: jid
        }, 'BucketManager::list done');

        self.bucketCache.reset();
        Object.keys(buckets).forEach(function (k) {
            self.bucketCache.set(k, buckets[k]);
        });
        return callback(null, buckets);
    });
};


/**
 * Lists keys under a bucket.
 *
 * Returns an object like:
 *
 * {
 *     keys: {
 *         $key: {
 *             etag: <etag>
 *             mtime: <mtime>
 *         }
 *     },
 *     total: <total number of matching keys, not subject to limit/offset>
 * }
 *
 * @arg {String} bucket - bucket name.
 * @arg {Object} options - parameterization object.
 * @arg {Number} options.limit - maximum number of keys to return.
 * @arg {Number} options.offset - offset to start looking at.
 * @arg {String} options.prefix - only keys matching that prefix are returned.
 * @arg {Function} callback - function (err, data).
 */
BucketManager.prototype.keys = function listKeys(bucket, options, callback) {
    assertString('bucket', bucket);
    if (typeof (options) === 'function') {
        callback = options;
        options = {};
    }
    assertObject('options', options);
    assertFunction('callback', callback);

    var jid = this.log.queryId();
    var log = this.log;
    var pg;
    var obj = {
        keys: {},
        total: 0
    };
    var limit = options.limit || 1000;
    var offset = options.offset || 0;
    var prefix = options.prefix || '';
    var self = this;


    log.debug({
        bucket: bucket,
        job_id: jid,
        limit: limit,
        offset: offset,
        prefix: prefix
    }, 'BucketManager::keys entered');

    return async.series([
        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function getTotal(cb) {
            var sql = sprintf(SQL.COUNT_KEYS, bucket, prefix);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return callback(err);

                obj.total = rows[0].count;
                return cb();
            });
        },

        function _listKeys(cb) {
            var sql = sprintf(SQL.LIST_KEYS, bucket, prefix, limit, offset);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return callback(err);

                obj.keys = rows;
                return cb();
            });
        },

        function rollback(cb) {
            return pg.rollback(cb);
        }

    ], function (err) {
        if (err) {
            return pg.rollback(function () {
                return callback(err);
            });
        }

        log.debug({
            bucket: bucket,
            job_id: jid,
            keys: obj
        }, 'BucketManager::keys done');
        return callback(null, obj);
    });
};


/**
 * Retrieves the schema and "triggers" for a bucket.
 *
 * Returned object looks like:
 * {
 *     schema: {
 *         ...
 *     },
 *     pre: [
 *         function ...
 *     ],
 *     post: [
 *         function ...
 *     ]
 * }
 *
 * @arg {String} bucket - bucket name.
 * @arg {Function} callback - function (err, bucket).
 */
BucketManager.prototype.get = function get(bucket, callback) {
    assertString('bucket', bucket);
    assertFunction('callback', callback);

    var b;
    var log = this.log;
    var self = this;

    if ((b = this.bucketCache.get(bucket))) {
        log.debug({bucket: b},
                  'BucketManager::get(%s) => found in cache',
                  bucket);
        // Unclear if this should be under a process.nextTick()
        return callback(null, b);
    }

    return this.list(function (err, buckets) {
        if (err)
            return callback(err);

        if (!buckets[bucket])
            return callback(new BucketNotFoundError(bucket));

        self.bucketCache.set(bucket, buckets[bucket]);
        return callback(null, buckets[bucket]);
    });
};


/**
 * Deletes a bucket.
 *
 * This is the equivalent of `rm -rf $bucket`, so make sure you really
 * want to do this.
 *
 * @param {String} bucket - bucket name.
 * @param {Function} callback - function (err).
 */
BucketManager.prototype.del = function del(bucket, callback) {
    assertString('bucket', bucket);
    assertFunction('callback', callback);

    var jid = this.log.queryId();
    var log = this.log;
    var pg;
    var self = this;

    log.debug({
        bucket: bucket,
        job_id: jid
    }, 'BucketManager::del entered');
    return async.series([
        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function dropEntryTable(cb) {
            return pg.query(sprintf(SQL.DROP_ENTRY_TABLE, bucket), cb);
        },

        function createTombstoneTable(cb) {
            return pg.query(sprintf(SQL.DROP_TOMBSTONE_TABLE, bucket), cb);
        },

        function deleteConfig(cb) {
            return pg.query(sprintf(SQL.DELETE_CONFIG, bucket), cb);
        },

        function commit(cb) {
            return pg.commit(cb);
        }

    ], function (err) {
        if (err) {
            if (pg) {
                return pg.rollback(function () {
                    return callback(err);
                });
            }
            return callback(err);
        }

        self.bucketCache.del(bucket);

        log.debug({
            bucket: bucket,
            job_id: jid
        }, 'BucketManager::del done');
        return callback(null);
    });
};



///--- Exports

module.exports = {
    BucketManager: BucketManager
};
