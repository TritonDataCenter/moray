/* Copyright (c) 2012 Joyent, Inc.  All rights reserved. */

var assert = require('assert');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var async = require('async');
var Logger = require('bunyan');
var poolModule = require('generic-pool');
var pg = require('pg');
var restify = require('restify');
var uuid = require('node-uuid');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var assertArgument = args.assertArgument;
var assertArray = args.assertArray;
var sprintf = util.format;



///--- Functions (helpers)

function internalError(e) {
    assert.ok(e);

    var err = new restify.InternalError(e.message);
    err.cause = e.stack;
    return err;
}


function goneError(bucket, key, dtime) {
    assert.ok(bucket);
    assert.ok(key);
    assert.ok(dtime);

    return new errors.ResourceGoneError(bucket, key, dtime);
}


function notFoundError(bucket, key) {
    assert.ok(bucket);
    assert.ok(key);

    return new restify.ResourceNotFoundError('urn:moray:%s:%s does not exist',
                                             bucket, key);
}

function entryTable(bucket) {
    assert.ok(bucket);
    return bucket + '_entry';
}


function tombstoneTable(bucket) {
    assert.ok(bucket);
    return bucket + '_tombstone';
}


function indexObject(indexes, object) {
    assert.ok(indexes);
    assert.ok(object);

    var ndx = {};

    if (indexes.length === 0)
        return ndx;

    function index(k, v) {
        switch (typeof (v)) {
        case 'boolean':
        case 'string':
        case 'number':
            ndx[k] = v + '';
            break;
        case 'object':
            if (v === null)
                return;
            if (Array.isArray(v)) {
                v.forEach(function (v2) {
                    return index(k, v2);
                });
            } else {
                Object.keys(v).forEach(function (k2) {
                    return index(k2, v[k2]);
                });
            }
            break;
        default:
            break;
        }
    }

    indexes.forEach(function (i) {
        return index(i, object[i]);
    });

    return ndx;
}


function waitForQuery(q, skipRows, callback) {
    assert.ok(q);
    if (typeof (skipRows) === 'function') {
        callback = skipRows;
        skipRows = false;
    }
    assert.ok(callback);

    var rows = [];
    if (!skipRows) {
        q.on('row', function (r) {
            rows.push(r);
        });
    }

    q.on('error', function (err) {
        return callback(internalError(err));
    });

    q.on('end', function () {
        return callback(null, !skipRows ? rows : undefined);
    });

    return q;
}


///--- API

/**
 * Creates a new (pooled) Postgres client.
 *
 <pre>
    var db = new DB({
        url: 'pg://unit:test@localhost/test',
        log: new Logger({
                stream: process.stderr,
                serializers: Logger.stdSerializers
        }),
        maxConns: 100,
        idleTimeout: 60000
    });
 </pre>
 *
 * @constructor
 * @arg {object} options             - Standard options object
 * @arg {object} options.log         - (bunyan) Logger instance.</br/>
 * @arg {string} options.url         - Postgres connection string.
 * @arg {number} options.maxConns    -  Maximum number of DB connections to
 *     maintain at any given time.
 * @arg {number} options.idleTimeout - Maximum time (in milliseconds) a
 *     connection can remain idle before it is reaped.
 *
 * @fires 'connect' - <pre>db.on('connect', function () {});</pre>
 * @fires 'error'   - <pre>db.on('error', function (err {});</pre>
 *
 * @throws {TypeError} on bad input types.
 */
function DB(options) {
    assertArgument('options', 'object', options);
    assertArgument('options.log', 'object', options.log);
    assertArgument('options.url', 'string', options.url);

    var self = this;

    EventEmitter.call(this);

    this._indexes = {};
    this.log = options.log;
    this.url = options.url;

    this.pool = poolModule.Pool({
        name     : 'postgres',
        create   : function create(callback) {
            var client = new pg.Client(self.url);
            client.connect(function (err) {
                if (err)
                    return callback(err);

                client.on('drain', function () {
                    self.emit('drain');
                });

                client.on('error', function (error) {
                    self.emit('error', internalError(error));
                });

                client.on('notice', function (message) {
                    self.log.trace('Postgres notice: %s', message);
                });

                self.__defineGetter__('client', function () {
                    return client;
                });

                client.rollback = function rollback(cb) {
                    return client.query('ROLLBACK', function () {
                        self.log.debug('Executing ROLLBACK');
                        return (typeof (cb) === 'function' ? cb() : false);
                    });
                };
                return callback(null, client);
            });

            client.on('error', function (err) {
                self.log.error({err: err}, 'Postgres client error');
                self.pool.destroy(client);
            });
        },
        destroy  : function destroy(client) {
            client.removeAllListeners('error');
            client.end();
        },
        max: options.maxConns || 10,
        idleTimeoutMillis: options.idleTimeout || 10000,
        log: function log(message, level) {
            var _log;
            if (level === 'warn') {
                _log = self.log.warn;
            } else if (level === 'error') {
                _log = self.log.error;
            } else {
                _log = self.log.trace;
            }

            _log.call(self.log, message);
        }
    });

    return this.buckets(function (err, _buckets) {
        if (err)
            return self.emit('error', err);

        self._indexes = _buckets;
        return self.emit('connect');
    });
}
util.inherits(DB, EventEmitter);
module.exports = DB;


/**
 * Shuts down the underlying postgres connection(s).
 *
 <pre>
    db.end(function (err) {
      assert.ifError(err);
      ...
    });
 </pre>
 * @arg {function} callback - only an error argument.
 */
DB.prototype.end = function end(callback) {
    var self = this;
    this.log.debug('end called');

    return this.pool.drain(function () {
        self.pool.destroyAllNow();
        return (typeof (callback) === 'function' ? callback() : false);
    });
};


/**
 * Lists all buckets in the system.
 *
 <p>
 The returned 'buckets' will be an object where the keys are the bucket names,
 and the values are all the indexes defined on the bucket.
 </p>
 *
 <pre>
    db.buckets(function (err, buckets) {
        assert.ifError(err);
        Object.keys(buckets).forEach(function (k) {
            console.log('%s:\t%s', k, buckets[k].join(', '));
        });
    });
 </pre>
 *
 * @arg {function} callback - arguments will be 'err' and 'buckets'.
 * @throws {TypeError} if callback is not a function.
 */
DB.prototype.buckets = function buckets(callback) {
    assertArgument('callback', 'function', callback);

    var sql = 'SELECT tablename, attname FROM pg_attribute, pg_type, ' +
        'pg_tables WHERE typname in ' +
        '(SELECT tablename from pg_tables WHERE schemaname=\'public\' AND ' +
        'tablename LIKE \'%_entry\')' +
        'AND attrelid = typrelid AND attname NOT IN (' +
        '\'cmin\', \'cmax\', \'ctid\', \'oid\', \'tableoid\', \'xmin\', ' +
        '\'xmax\') AND typname = tablename';

    var self = this;
    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        return waitForQuery(self.query(client, sql), function (err, rows) {
            self.pool.release(client);

            if (err)
                return callback(err);

            var obj = {};
            rows.forEach(function (r) {
                var tmp = r.tablename.split('_entry');
                if (tmp.length !== 2)
                    return;

                var name = tmp[0];
                if (obj[name] === undefined)
                    obj[name] = [];

                switch (r.attname) {
                case 'id':
                case 'key':
                case 'value':
                case 'etag':
                case 'mtime':
                    // noop
                    break;
                default:
                    if (obj[name].indexOf(r.attname) === -1)
                        obj[name].push(r.attname);
                    break;
                }
            });

            return callback(null, obj);
        });
    });
};


/**
 * Creates a new bucket.
 *
 <pre>
    db.createBucket('foo', ['name', 'email'], function (err) {
        assert.ifError(err);
        ...
    });
 </pre>
 *
 * @arg {string} bucket         - The name of the bucket to create (globally).
 * @arg {array[string]} indexes - An array of attributes to index.
 * @arg {function} callback     - `function (err) {}`.
 * @throws {TypeError} if bucket is not a string.
 */
DB.prototype.createBucket = function createBucket(bucket, indexes, callback) {
    assertArgument('bucket', 'string', bucket);
    if (typeof (indexes) === 'function') {
        callback = indexes;
        indexes = [];
    }
    assertArray('indexes', 'string', indexes);
    assertArgument('callback', 'function', callback);

    var queries = ['BEGIN'];
    var sql;
    var self = this;

    // entry table
    sql = 'CREATE TABLE IF NOT EXISTS ' + entryTable(bucket) + ' (' +
        'id  SERIAL PRIMARY KEY, ' +
        'key TEXT NOT NULL UNIQUE, ' +
        'value TEXT NOT NULL, ' +
        'etag CHAR(32) NOT NULL, ';
    indexes.forEach(function (i) {
        sql += i + ' TEXT, ';
    });
    sql += 'mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)';
    queries.push(sql);

    sql = 'CREATE TABLE IF NOT EXISTS ' + tombstoneTable(bucket) + ' (' +
        'key TEXT PRIMARY KEY, ' +
        'value TEXT NOT NULL, ' +
        'etag CHAR(32) NOT NULL, ';
    indexes.forEach(function (i) {
        sql += i + ' TEXT, ';
    });
    sql += 'dtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)';
    queries.push(sql);

    // Indexes
    indexes.forEach(function (i) {
        queries.push('DROP INDEX IF EXISTS ' + entryTable(bucket) + '_' +
                     i + '_idx');
        queries.push('CREATE INDEX ' + entryTable(bucket) + '_' + i +
                     '_idx ON ' + entryTable(bucket) +
                     '(' + i + ') WHERE ' + i + ' IS NOT NULL');
    });
    queries.push('COMMIT');

    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        return async.forEachSeries(queries, function (q, cb) {
            return waitForQuery(self.query(client, q), cb);
        }, function (err) {
            self.pool.release(client);

            if (err)
                return callback(internalError(err));

            if (indexes.length)
                self._indexes[bucket] = indexes;

            return callback(null, self._indexes[bucket]);
        });
    });
};


/**
 * Deletes a bucket and all data in it.
 *
 <pre>
    db.deleteBucket('foo', function (err) {
        assert.ifError(err);
        ...
    });
 </pre>
 *
 * @arg {string} bucket     - the name of the bucket to destroy.
 * @arg {function} callback - only argument is an error argument.
 * @throws {TypeError} on bad input.
 */
DB.prototype.deleteBucket = function deleteBucket(bucket, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('callback', 'function', callback);

    var queries = [
        'BEGIN',
        'DROP TABLE IF EXISTS ' + tombstoneTable(bucket),
        'DROP TABLE IF EXISTS ' + entryTable(bucket),
        'COMMIT'
    ];
    var self = this;

    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        return async.forEachSeries(queries, function (q, cb) {
            return waitForQuery(self.query(client, q), cb);
        }, function (err) {
            self.pool.release(client);

            if (err)
                return callback(err);

            return callback();
        });
    });
};


/**
 * Idempotently writes a key/value pair to the bucket.
 *
 * Note that if the key does not exist, it is added.  If the key did exist, the
 * value is overwritte, and the previous value is saved to the tombstone.  You
 * can revive it with the restore API.
 *
 <pre>
    db.put('foo', 'mykey', {name: 'mcavage'}, function (err) {
        assert.ifError(err);
        ...
    });
 </pre>
 *
 * @arg {string} bucket     - name of the bucket to write the k/v to.
 * @arg {string} key        - name of the key to write.
 * @arg {object} value      - arbitrary JS object to save.
 * @arg {function} callback - only argument is an error.
 * @throws {TypeError} on bad input.
 */
DB.prototype.put = function put(bucket, key, value, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assertArgument('value', 'object', value);
    assertArgument('callback', 'function', callback);

    var client;
    var data = JSON.stringify(value);
    var etag = crypto.createHash('md5').update(data).digest('hex');
    var self = this;
    var skipTombstone = false;

    var tasks = [
        function startTxn(cb) {
            return waitForQuery(self.query(client, 'BEGIN'), cb);
        },

        function loadRows(cb) {
            var sql = sprintf('SELECT * FROM %s WHERE key=\'%s\' FOR UPDATE',
                              entryTable(bucket), key);

            return waitForQuery(self.query(client, sql), function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length === 0)
                    skipTombstone = true;

                return cb();
            });
        },

        function dropExistingTombstone(cb) {
            if (skipTombstone)
                return cb();

            var sql = sprintf('DELETE FROM %s WHERE key=\'%s\'',
                              tombstoneTable(bucket), key);

            return waitForQuery(self.query(client, sql), cb);
        },

        function tombstone(cb) {
            if (skipTombstone)
                return cb();

            var sql = sprintf('WITH moved_rows AS (\n' +
                              '    DELETE FROM %s\n' +
                              '    WHERE\n' +
                              '        key=\'%s\'\n' +
                              'RETURNING key, value, etag',
                              entryTable(bucket), key);

            self._indexes[bucket].forEach(function (i) {
                sql += ', ' + i;
            });
            sql += sprintf(')\nINSERT INTO %s SELECT * FROM moved_rows',
                           tombstoneTable(bucket));

            return waitForQuery(self.query(client, sql), cb);
        },

        function insert(cb) {
            var pre = sprintf('INSERT INTO %s (key, value, etag',
                              entryTable(bucket));
            var post = ') VALUES ($1, $2, $3';

            var indexes = indexObject(self._indexes[bucket], value);
            var keys = Object.keys(indexes);
            var vals = [key, data, etag];

            for (var i = 0; i < keys.length; i++) {
                pre += ', ' + keys[i];
                post += ', $' + (3 + (i + 1));
                vals.push(indexes[keys[i]]);
            }

            return waitForQuery(self.query(client, pre + post + ')', vals), cb);
        },

        function commit(cb) {
            return waitForQuery(self.query(client, 'COMMIT'), cb);
        }
    ];

    return this.pool.acquire(function (poolErr, dbClient) {
        if (poolErr)
            return callback(internalError(poolErr));

        client = dbClient;
        return async.series(tasks, function (err) {
            self.pool.release(client);

            if (err)
                return callback(err);

            return callback(null, etag);
        });
    });
};


/**
 * Retrieves a key/value pair from the DB (if it exists).
 *
 * The returned object will include the value, bucket name, etag and mtime.
 *
 <pre>
    db.get('foo', 'mykey', function (err, obj) {
        assert.ifError(err);
        console.log('bucket:\t%s', obj.bucket);
        console.log('key:\t%s', obj.key);
        console.log('value:\t%j', obj.data);
        console.log('etag:\t%s', obj.etag);
        console.log('mtime:\t%s', obj.mtime);
    });
 </pre>
 *
 * @arg {string} bucket     - name of the bucket to read from.
 * @arg {string} key        - name of the key to get.
 * @arg {function} callback - first argument is possible error, second is the
 *   value payload.
 * @throws {TypeError} on bad input.
 */
DB.prototype.get = function get(bucket, key, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assertArgument('callback', 'function', callback);

    var self = this;
    var sql = sprintf('SELECT key, value, etag, mtime FROM %s ' +
                      'WHERE key=\'%s\'', entryTable(bucket), key);

    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        return waitForQuery(self.query(client, sql), function (err, rows) {
            if (err) {
                self.pool.release(client);
                return callback(internalError(err));
            }

            // Check if we need to return a 410 (i.e., the key was tombstoned)
            if (rows.length === 0) {
                var sql2 = sprintf('SELECT dtime FROM %s WHERE key=\'%s\'',
                                   tombstoneTable(bucket), key);
                var tq = self.query(client, sql2);
                return waitForQuery(tq, function (err2, rows2) {
                    self.pool.release(client);
                    if (err2)
                        return callback(err2);

                    if (rows2.length === 0)
                        return callback(notFoundError(bucket, key));

                    return callback(goneError(bucket, key, rows2[0].dtime));
                });
            }

            self.pool.release(client);

            var obj;
            try {
                assert.equal(rows.length, 1);
                obj = rows[0];
                obj.bucket = bucket;
                obj.data = JSON.parse(rows[0].value);
                obj.mtime = new Date(rows[0].mtime);
                delete obj.value;
            } catch (e) {
                return callback(internalError(e));
            }

            return callback(null, obj);
        });
    });
};


/**
 * Deletes a key/value pair from the DB (if it exists).
 *
 * If successful, the existing value is moved into the tombstone.
 *
 <pre>
    db.del('foo', 'mykey', function (err) {
        assert.ifError(err);
    });
 </pre>
 *
 * @arg {string} bucket     - name of the bucket to act on.
 * @arg {string} key        - name of the key to delete.
 * @arg {function} callback - only argument is possible error.
 * @throws {TypeError} on bad input.
 */
DB.prototype.del = function del(bucket, key, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assertArgument('callback', 'function', callback);

    var client;
    var self = this;

    var tasks = [
        function startTxn(cb) {
            return waitForQuery(self.query(client, 'BEGIN'), cb);
        },

        function loadRows(cb) {
            var sql = sprintf('SELECT * FROM %s WHERE key=\'%s\' FOR UPDATE',
                              entryTable(bucket), key);

            var r = waitForQuery(self.query(client, sql), function (err, rows) {
                if (err) {
                    return cb(internalError(err));
                } else if (rows.length === 0) {
                    r.rollback(function () {
                        return cb(notFoundError(bucket, key));
                    });
                }

                return cb();
            });
        },

        function dropExistingTombstone(cb) {
            var sql = sprintf('DELETE FROM %s WHERE key=\'%s\'',
                              tombstoneTable(bucket), key);
            return waitForQuery(self.query(client, sql), cb);
        },

        function tombstone(cb) {
            var sql = sprintf('WITH moved_rows AS (\n' +
                              '    DELETE FROM %s\n' +
                              '    WHERE\n' +
                              '        key=\'%s\'\n' +
                              'RETURNING key, value, etag',
                              entryTable(bucket), key);

            self._indexes[bucket].forEach(function (i) {
                sql += ', ' + i;
            });
            sql += sprintf(')\nINSERT INTO %s SELECT * FROM moved_rows',
                           tombstoneTable(bucket));

            return waitForQuery(self.query(client, sql), cb);
        },

        function commit(cb) {
            return waitForQuery(self.query(client, 'COMMIT'), cb);
        }
    ];

    return this.pool.acquire(function (poolErr, dbClient) {
        if (poolErr)
            return callback(internalError(poolErr));

        client = dbClient;
        return async.series(tasks, function (err) {
            self.pool.release(client);

            if (err)
                return callback(err);

            return callback();
        });
    });
};


/**
 * Restores a value from the tombstone for a key.
 *
 * If successful, the existing value (if there was one) is moved into
 * the tombstone.  The old value will be "live".
 *
 <pre>
    db.restore('foo', 'mykey', function (err) {
        assert.ifError(err);
    });
 </pre>
 *
 * @arg {string} bucket     - name of the bucket to act on.
 * @arg {string} key        - name of the key to restore.
 * @arg {function} callback - only argument is possible error.
 * @throws {TypeError} on bad input.
 */
DB.prototype.restore = function restore(bucket, key, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assertArgument('callback', 'function', callback);

    var client;
    var self = this;
    var save = [];
    var tasks = [
        function startTxn(cb) {
            return waitForQuery(self.query(client, 'BEGIN'), cb);
        },

        function loadTombstoneRows(cb) {
            var sql = sprintf('SELECT * FROM %s WHERE key=\'%s\' FOR UPDATE',
                              tombstoneTable(bucket), key);

            return waitForQuery(self.query(client, sql), function (err, rows) {
                if (err)
                    return cb(err);

                if (rows.length === 0)
                    return cb(notFoundError(bucket, key + ':tombstone'));

                return cb();
            });
        },

        function saveEntryRows(cb) {
            var sql = sprintf('DELETE FROM %s WHERE key=\'%s\' RETURNING *',
                              entryTable(bucket), key);

            return waitForQuery(self.query(client, sql), function (err, rows) {
                if (err)
                    return cb(err);

                save = rows;
                return cb();
            });
        },

        function restoreTombstone(cb) {
            var sql = sprintf('WITH moved_rows AS (\n' +
                              '    DELETE FROM %s\n' +
                              '    WHERE\n' +
                              '        key=\'%s\'\n' +
                              'RETURNING key, value, etag',
                              tombstoneTable(bucket), key);

            self._indexes[bucket].forEach(function (i) {
                sql += ', ' + i;
            });
            sql += sprintf(')\nINSERT INTO %s(key, value, etag',
                           entryTable(bucket));
            self._indexes[bucket].forEach(function (i) {
                sql += ', ' + i;
            });
            sql += ') SELECT * FROM moved_rows';
            return waitForQuery(self.query(client, sql), cb);
        },

        function archive(cb) {
            if (save.length === 0)
                return cb();

            assert.ok(save.length, 1);
            var entry = save[0];
            delete entry.id;
            delete entry.mtime;

            var pre = sprintf('INSERT INTO %s (', tombstoneTable(bucket));
            var post = ' VALUES (';
            var keys = Object.keys(entry);
            var vals = [];
            for (var i = 0; i < keys.length; i++) {
                pre += keys[i];
                post += '$' + (i + 1);
                if (i < keys.length - 1) {
                    pre += ', ';
                    post += ', ';
                }
                vals.push(entry[keys[i]]);
            }
            pre += ')';
            post += ')';

            return waitForQuery(self.query(client, pre + post, vals), cb);
        },

        function read(cb) {
            var sql = sprintf('SELECT * FROM %s WHERE key=\'%s\'',
                              entryTable(bucket), key);

            return waitForQuery(self.query(client, sql), function (err, rows) {
                if (err)
                    return cb(err);

                assert.ok(rows.length, 1);
                save = rows[0];
                return cb();
            });
        },

        function commit(cb) {
            return waitForQuery(self.query(client, 'COMMIT'), cb);
        }
    ];

    return this.pool.acquire(function (poolErr, dbClient) {
        if (poolErr)
            return callback(internalError(poolErr));

        client = dbClient;
        return async.series(tasks, function (err) {
            self.pool.release(client);

            if (err) {
                return client.rollback(function () {
                    return callback(err);
                });
            }

            var obj;
            try {
                obj = save;
                obj.bucket = bucket;
                obj.data = JSON.parse(save.value);
                obj.mtime = new Date(save.mtime);
                delete obj.value;
            } catch (e) {
                return callback(internalError(e));
            }

            return callback(null, obj);
        });
    });
};


/**
 * Finds a key/value by secondary index.
 *
 * You can only search for attributes that were previously indexed.
 *
 <pre>
    db.find('foo', 'name', 'mcavage', function (err, query) {
        assert.ifError(err);
        query.on('error', function (err) {
            console.error('find error: %s', err.stack);
        });
        query.on('entry', function (obj) {
            t.ok(obj);
            console.log('bucket:\t%s', obj.bucket);
            console.log('key:\t%s', obj.key);
            console.log('value:\t%j', obj.data);
            console.log('etag:\t%s', obj.etag);
            console.log('mtime:\t%s', obj.mtime);
        });
        query.on('end', function () {
            ...
        });
    });
 </pre>
 *
 * @arg {string} bucket     - name of the bucket to read from.
 * @arg {string} attribute  - name of the index to search.
 * @arg {string} value      - value to match.
 * @arg {function} callback - first argument is possible error, second is an
 *   EventEmitter, which has 'error', 'entry' and 'end' events.
 * @throws {TypeError} on bad input.
 */
DB.prototype.find = function find(bucket, attribute, value, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('attribute', 'string', attribute);
    assertArgument('value', 'string', value);
    assertArgument('callback', 'function', callback);

    var self = this;
    var sql = sprintf('SELECT %s FROM %s WHERE %s LIKE \'%%%s%\'',
                      'key, value, etag, mtime',
                      entryTable(bucket),
                      attribute,
                      value);

    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        var req = self.query(client, sql);
        req.on('row', function (row) {
            try {
                row.bucket = bucket;
                row.data = JSON.parse(row.value);
            } catch (e) {
                req.emit('error', e);
            }
            req.emit('entry', row);
        });
        req.on('error', function (err) {
            self.pool.release(client);
        });
        req.on('end', function () {
            self.pool.release(client);
        });
        return callback(null, req);
    });
};


DB.prototype.query = function query(client, sql, values) {
    assertArgument('client', 'object', client);
    assertArgument('sql', 'string', sql);
    if (values !== undefined)
        assertArray('values', null, values);

    var obj = new EventEmitter();
    var qid = uuid().substr(0, 7);
    var req;

    obj.log = this.log;
    obj.rollback = function rollback(callback) {
        return client.rollback(callback);
    };

    obj.log.debug({
        query_id: qid,
        sql: sql,
        values: values || null
    }, 'query starting');

    // Create the request, and wrap all of the events that can come back
    // with logging statements
    req = client.query(sql, values);

    req.on('error', function (err) {
        obj.log.debug({err: err, query_id: qid}, 'query error');
        obj.rollback(function () {
            obj.emit('error', err);
        });
    });

    req.on('row', function (row) {
        obj.log.debug({row: row, query_id: qid}, 'row received');
        obj.emit('row', row);
    });

    req.on('end', function () {
        obj.log.debug({query_id: qid}, 'query complete');
        obj.emit('end');
    });

    return obj;
};
