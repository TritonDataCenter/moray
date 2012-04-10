// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var async = require('async');
var ldap = require('ldapjs');

var errors = require('../errors');
var SQL = require('./sql');

require('../args');



///--- Globals

var sprintf = util.format;



///--- Helpers

function indexObject(bucket, schema, object) {
    assert.ok(bucket);
    assert.ok(schema);
    assert.ok(object);

    var ndx = {};

    if (Object.keys(schema).length === 0)
        return ndx;

    function index(k, v, t) {
        if (v !== undefined && typeof (v) !== t) {
            if (typeof (v) === 'object') {
                if (Array.isArray(v)) {
                    return v.forEach(function (v2) {
                        return index(k, v2, t);
                    });
                } else if (v === null) {
                    return false;
                }
                throw new IndexTypeError(bucket, k, t);
            }
        }

        switch (typeof (v)) {
        case 'boolean':
        case 'string':
        case 'number':
            ndx[k] = v + '';
            break;
        default:
            break;
        }

        return true;
    }

    Object.keys(schema).forEach(function (k) {
        return index(k, object[k], schema[k].type);
    });

    return ndx;
}


function rowToObject(bucket, key, row) {
    assertString('bucket', bucket);
    assertString('key', key);
    assertObject('row', row);

    var obj = {
        bucket: bucket,
        key: key,
        value: JSON.parse(row.value),
        etag: row.etag
    };

    if (row.dtime)
        obj.dtime = new Date(row.dtime);
    if (row.mtime)
        obj.mtime = new Date(row.mtime);

    return obj;
}


function compileQuery(bucket, schema, query) {
    assertString('bucket', bucket);
    assertObject('schema', schema);
    assertString('query', query);

    function value(k, v) {
        if (!schema[k])
            throw new NotIndexedError(bucket, k);

        switch (schema[k].type) {
        case 'boolean':
            return v ? 'true' : 'false';

        case 'number':
            return v;

        default:
            return '\'' + v + '\'';
        }
    }

    var f = ldap.parseFilter(query);
    var where = '';

    switch (f.type) {
    case 'and':
    case 'or':
        where += '(';
        for (var i = 0; i < f.filters.length; i++) {
            where += compileQuery(bucket, schema, f.filters[i].toString());
            if (i < f.filters.length - 1)
                where += ' ' + f.type.toUpperCase() + ' ';
        }
        where += ')';
        break;

    case 'not':
        where += ' (NOT (' +
            compileQuery(bucket, schema, f.filter.toString()) +
            '))';
        break;

    case 'ge':
        where += f.attribute + ' >= ' + value(f.attribute, f.value);
        break;

    case 'le':
        where += f.attribute + ' <= ' + value(f.attribute, f.value);
        break;

    case 'present':
        where += f.attribute + ' IS NOT NULL';
        break;

    case 'substring':
        where += f.attribute + ' LIKE \'';
        if (f.initial)
            where += f.initial + '%';
        f.any.forEach(function (s) {
            where += '%' + s + '%';
        });
        if (f['final'])
            where += '%' + f['final'];

        where += '\'';
        break;

    case 'equal':
    default:
        where += f.attribute + ' = ' + value(f.attribute, f.value);
        break;
    }

    return where;
}



///--- API

/**
 * Constructs a new ObjectManager.
 *
 * @arg {Object} options - configuration.
 * @arg {Object} options.bucketManager - bucketManager instance.
 * @arg {Object} options.log - bunyan logger
 * @arg {Object} options.pgClient - postgres client.
 * @constructor
 */
function ObjectManager(options) {
    assertObject('options', options);
    assertObject('options.pgClient', options.pgClient);
    assertObject('options.bucketManager', options.bucketManager);
    assertObject('options.log', options.log);

    EventEmitter.call(this);

    this.bm = options.bucketManager;
    this.log = options.log;
    this.pgClient = options.pgClient;

    var self = this;
    this.__defineGetter__('bucketManager', function () {
        return self.bm;
    });
}
util.inherits(ObjectManager, EventEmitter);


/**
 * Idempotently writes a key/value pair to a bucket.
 *
 * Note that if the key does not exist, it is added.  If the key did exist, the
 * value is overwritte, and the previous value is saved to the tombstone.  You
 * can revive it with the restore API.
 *
 *
 *  mgr.put('foo', 'key', {name: 'mcavage'}, function (err) {
 *      assert.ifError(err);
 *      ...
 *  });
 *
 *
 * @arg {String} bucket     - name of the bucket to write the k/v to.
 * @arg {String} key        - name of the key to write.
 * @arg {Object} value      - arbitrary JS object to save.
 * @arg {Object} etag       - (optional) etag to enforce
 * @arg {String} etag.value - match against $row.etag.
 * @arg {Boolean} etag.match - corresponds to If-Match/If-None-Match
 * @arg {function} callback - function (err).
 */
ObjectManager.prototype.put = function put(bucket, key, value, etag, callback) {
    assertString('bucket', bucket);
    assertString('key', key);
    assertObject('value', value);
    if (typeof (etag) === 'function') {
        callback = etag;
        etag = {};
    }
    assertObject('etag', etag);
    assertFunction('callback', callback);

    var _bucket;
    var data = JSON.stringify(value);
    var _etag = crypto.createHash('md5').update(data).digest('hex');
    var jid = this.log.queryId();
    var log = this.log;
    var pg;
    var save;
    var self = this;
    var tombstone = true;

    log.debug({
        bucket: bucket,
        key: key,
        value: value,
        etag: etag,
        job_id: jid
    }, 'ObjectManager::put entered');

    return async.series([
        // Note, in the worst case, this will spawn a new transaction, but
        // 99.99% of the time (or something) it will be cached in-memory.
        function loadBucket(cb) {
            return self.bm.get(bucket, function (err, b) {
                if (err)
                    return cb(err);

                _bucket = b;
                return cb();
            });
        },

        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function loadCurrentEntry(cb) {
            var sql = sprintf(SQL.SELECT_ENTRY_FOR_UPDATE, bucket, key);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length === 0) {
                    tombstone = false;
                } else {
                    save = rows;
                }
                return cb();
            });
        },

        function enforceEtag(cb) {
            if (etag.value === undefined)
                return cb();

            if (save.length === 0)
                return cb(new EtagConflictError(bucket, key, 'does not exist'));

            var matches = save[0].etag === etag.value;
            if (matches !== etag.match)
                return cb(new EtagConflictError(bucket, key, save[0].etag));

            return cb();
        },

        function dropTombstone(cb) {
            if (!tombstone)
                return cb();

            return pg.query(sprintf(SQL.DELETE_TOMBSTONE, bucket, key), cb);
        },

        function tombstoneEntry(cb) {
            if (!tombstone)
                return cb();

            var str = '';
            Object.keys(_bucket.schema).forEach(function (k) {
                str += ', ' + k;
            });
            var sql =
                sprintf(SQL.TOMBSTONE_ENTRY, bucket, key, str, bucket, str);
            return pg.query(sql, cb);
        },

        function runPreChain(cb) {
            if (_bucket.pre.length === 0)
                return cb();

            var preRequest = {
                bucket: bucket,
                key: key,
                schema: _bucket.schema,
                value: value,
                pgClient: pg,
                bucketManager: self.bm,
                objectManager: self
            };

            var tasks = [];
            _bucket.pre.forEach(function (fn) {
                tasks.push(function call(_cb) {
                    if (typeof (fn) === 'string')
                        assert.ok(eval('fn = ' + fn));
                    return fn(preRequest, _cb);
                });
            });

            return async.series(tasks, function (err, results) {
                if (err)
                    return cb(err);

                if (results) {
                    results.forEach(function (r) {
                        if (typeof (r) === 'object') {
                            if (r.bucket)
                                bucket = r.bucket;
                            if (r.key)
                                key = r.key;
                            if (r.value)
                                data = JSON.stringify(r.value);
                            if (r.etag)
                                _etag = r.etag;
                        }
                    });
                }

                return cb();
            });
        },

        function insert(cb) {
            var values = [key, data, _etag];
            var keyStr = '';
            var valStr = '';

            try {
                var ndx = indexObject(bucket, _bucket.schema, value);
            } catch (e) {
                return cb(new BadRequestError(e.message));
            }
            var keys = Object.keys(ndx);
            for (var i = 0; i < keys.length; i++) {
                keyStr += ', ' + keys[i];
                valStr += ', $' + (i + 4);
                values.push(ndx[keys[i]]);
            }

            var sql = sprintf(SQL.INSERT_ENTRY, bucket, keyStr, valStr);
            return pg.query(sql, values, cb);
        },

        function runPostChain(cb) {
            if (_bucket.post.length === 0)
                return cb();

            var postRequest = {
                bucket: bucket,
                key: key,
                schema: _bucket.schema,
                value: value,
                pgClient: pg
            };

            var tasks = [];
            _bucket.post.forEach(function (fn) {
                tasks.push(function call(_cb) {
                    if (typeof (fn) === 'string')
                        assert.ok(eval('fn = ' + fn));
                    return fn(postRequest, _cb);
                });
            });

            return async.series(tasks, function (err) {
                if (err)
                    return cb(err);

                return cb();
            });
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

        log.debug({bucket: bucket,
                   key: key,
                   value: value,
                   job_id: jid}, 'ObjectManager::put done');
        return callback(null, _etag);
    });
};


/**
 * Retrieves a key/value pair from the DB (if it exists).
 *
 * The returned object will include the value, bucket name, etag and mtime.
 *
 *  mgr.get('foo', 'mykey', function (err, obj) {
 *      assert.ifError(err);
 *      ...
 *  });
 *
 *
 * @arg {string} bucket     - name of the bucket to read from.
 * @arg {String} key        - name of the key to get.
 * @arg {Function} callback - function (err, object).
 */
ObjectManager.prototype.get = function get(bucket, key, callback) {
    assertString('bucket', bucket);
    assertString('key', key);
    assertFunction('callback', callback);

    var log = this.log;
    var jid = this.log.queryId();
    var pg;
    var obj;
    var self = this;

    log.debug({
        bucket: bucket,
        key: key,
        job_id: jid
    }, 'ObjectManager::get entered');

    return async.series([
        function ensureBucket(cb) {
            return self.bm.get(bucket, cb);
        },

        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function getEntry(cb) {
            var sql = sprintf(SQL.GET_ENTRY, bucket, key);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                // Check tombstone table for GONE
                if (rows.length === 0)
                    return cb();

                try {
                    obj = rowToObject(bucket, key, rows[0]);
                } catch (e) {
                    return cb(e);
                }

                return cb();
            });
        },

        function checkTombstone(cb) {
            if (obj)
                return cb();

            var sql = sprintf(SQL.GET_TOMBSTONE_ENTRY, bucket, key);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length === 0)
                    return cb(new ObjectNotFoundError(bucket, key));

                return cb(new ResourceGoneError(bucket, key, rows[0].dtime));
            });
        },

        function rollback(cb) {
            pg.rollback(cb);
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

        log.debug({bucket: bucket,
                   key: key,
                   object: obj,
                   job_id: jid}, 'ObjectManager::get done');
        return callback(null, obj);
    });
};


/**
 * Deletes a key/value pair from the DB (if it exists).
 *
 * If successful, the existing value is moved into the tombstone.
 *
 * mgr.del('foo', 'mykey', function (err) {
 *     assert.ifError(err);
 * });
 *
 * @arg {String} bucket     - name of the bucket to act on.
 * @arg {String} key        - name of the key to delete.
 * @arg {Function} callback - function (err).
 */
ObjectManager.prototype.del = function del(bucket, key, callback) {
    assertString('bucket', bucket);
    assertString('key', key);
    assertFunction('callback', callback);

    var _bucket;
    var log = this.log;
    var jid = this.log.queryId();
    var pg;
    var self = this;

    log.debug({
        bucket: bucket,
        key: key,
        job_id: jid
    }, 'ObjectManager::del entered');

    return async.series([
        function ensureBucket(cb) {
            return self.bm.get(bucket, function (err, b) {
                if (err)
                    return cb(err);

                _bucket = b;
                return cb();
            });
        },

        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function dropTombstone(cb) {
            return pg.query(sprintf(SQL.DELETE_TOMBSTONE, bucket, key), cb);
        },

        function tombstone(cb) {
            var str = '';
            Object.keys(_bucket.schema).forEach(function (k) {
                str += ', ' + k;
            });
            var sql = sprintf(SQL.TOMBSTONE_ENTRY,
                              bucket, key, str, bucket, str);
            return pg.query(sql, cb);
        },

        function commit(cb) {
            pg.commit(cb);
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

        log.debug({bucket: bucket,
                   key: key,
                   job_id: jid}, 'ObjectManager::del done');
        return callback(null);
    });
};


/**
 * Retrieves a key/value pair from the tombstone (if it exists)
 *
 * The returned object will include the value, bucket name, etag and mtime.
 *
 *  mgr.getTombstone('foo', 'mykey', function (err, obj) {
 *      assert.ifError(err);
 *      ...
 *  });
 *
 *
 * @arg {string} bucket     - name of the bucket to read from.
 * @arg {String} key        - name of the key to get.
 * @arg {Function} callback - function (err, object).
 */
ObjectManager.prototype.getTombstone = function getTombstone(bucket,
                                                             key,
                                                             callback) {
    assertString('bucket', bucket);
    assertString('key', key);
    assertFunction('callback', callback);

    var log = this.log;
    var jid = this.log.queryId();
    var obj;
    var self = this;

    log.debug({
        bucket: bucket,
        key: key,
        job_id: jid
    }, 'ObjectManager::getTombstone entered');

    return async.series([
        function ensureBucket(cb) {
            return self.bm.get(bucket, cb);
        },

        function checkTombstone(cb) {
            var sql = sprintf(SQL.GET_TOMBSTONE_ENTRY, bucket, key);
            return self.pgClient.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length === 0)
                    return cb(new TombstoneNotFoundError(bucket, key));

                try {
                    obj = rowToObject(bucket, key, rows[0]);
                } catch (e) {
                    return cb(e);
                }

                return cb();
            });
        }
    ], function (err) {
        if (err)
            return callback(err);

        log.debug({bucket: bucket,
                   key: key,
                   object: obj,
                   job_id: jid}, 'ObjectManager::getTombstone done');
        return callback(null, obj);
    });
};


/**
 * Restores a value from the tombstone for a key.
 *
 * If successful, the existing value (if there was one) is moved into
 * the tombstone.  The old value will be "live".
 *
 * mgr.restore('foo', 'mykey', function (err, obj) {
 *     assert.ifError(err);
 *  });
 *
 * @arg {String} bucket     - name of the bucket to act on.
 * @arg {String} key        - name of the key to restore.
 * @arg {Function} callback - function (err, object).
 */
ObjectManager.prototype.restore = function restore(bucket, key, callback) {
    assertString('bucket', bucket);
    assertString('key', key);
    assertFunction('callback', callback);

    var _bucket;
    var log = this.log;
    var jid = this.log.queryId();
    var pg;
    var obj;
    var save;
    var self = this;

    log.debug({
        bucket: bucket,
        key: key,
        job_id: jid
    }, 'ObjectManager::restore entered');

    return async.series([
        function ensureBucket(cb) {
            return self.bm.get(bucket, function (err, b) {
                if (err)
                    return cb(err);

                _bucket = b;
                return cb();
            });
        },

        function start(cb) {
            return self.pgClient.start(function (err, conn) {
                if (err)
                    return cb(err);

                pg = conn;
                return cb();
            });
        },

        function loadTombstoneRows(cb) {
            var sql = sprintf(SQL.SELECT_TOMBSTONE_FOR_UPDATE, bucket, key);
            return pg.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length === 0)
                    return cb(new TombstoneNotFoundError(bucket, key));

                save = rows[0];
                return cb();
            });
        },

        function dropTombstone(cb) {
            return pg.query(sprintf(SQL.DELETE_TOMBSTONE, bucket, key), cb);
        },

        function tombstone(cb) {
            var str = '';
            Object.keys(_bucket.schema).forEach(function (k) {
                str += ', ' + k;
            });
            var sql = sprintf(SQL.TOMBSTONE_ENTRY,
                              bucket, key, str, bucket, str);
            return pg.query(sql, cb);
        },

        function restoreEntry(cb) {
            var i = 4;
            var str = '';
            var valStr = '';
            var values = [key, save.value, save.etag];
            Object.keys(_bucket.schema).forEach(function (k) {
                if (save[k]) {
                    str += ', ' + k;
                    valStr += ', $' + (i++);
                    values.push(save[k]);
                }
            });
            var sql = sprintf(SQL.INSERT_ENTRY, bucket, str, valStr);
            return pg.query(sql, values, cb);
        },

        function transformObject(cb) {
            try {
                obj = rowToObject(bucket, key, save);
            } catch (e) {
                return cb(e);
            }

            return cb();
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

        log.debug({bucket: bucket,
                   key: key,
                   object: obj,
                   job_id: jid}, 'ObjectManager::restore done');
        return callback(null, obj);
    });
};


ObjectManager.prototype.find = function find(bucket, query, callback) {
    assertString('bucket', bucket);
    assertString('query', query);
    assertFunction('callback', callback);

    var _bucket;
    var clause;
    var results;
    var self = this;

    return async.series([
        function ensureBucket(cb) {
            return self.bm.get(bucket, function (err, b) {
                if (err)
                    return cb(err);

                _bucket = b;
                return cb();
            });
        },

        function buildWhereClause(cb) {
            try {
                clause = compileQuery(bucket, _bucket.schema, query);
            } catch (e) {
                return cb(e);
            }
            return cb();
        },

        function findRecords(cb) {
            var sql = sprintf(SQL.FIND_ENTRIES, bucket, clause);
            return self.pgClient.query(sql, function (err, rows) {
                if (err)
                    return cb(err);

                results = rows.map(function (r) {
                    return rowToObject(bucket, r.key, r);
                });

                return cb();
            });
        }

    ], function (err) {
        if (err)
            return callback(err);

        return callback(null, results || []);
    });
};



///--- Exports

module.exports = {
    ObjectManager: ObjectManager
};
