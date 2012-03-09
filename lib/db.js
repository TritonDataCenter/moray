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

    var err = new InternalError(e.message);
    err.cause = e.stack;
    return err;
}


function pgError(e) {
    assert.ok(e);

    var err;
    var msg;

    console.log(e);
    switch (e.code) {
    case '22P02':
        err = new InvalidArgumentError(e.message);
        break;
    case '23505':
        /* JSSTYLED */
        msg = /.*\((\w+)\)=\((\w+)\).*/.exec(e.detail);
        err = new InvalidArgumentError(msg ?
                                       msg[1] + ' is a unique ' +
                                       'attribute and value \'' +
                                       msg[2] + '\' already exists.'
                                       : e.detail);
        break;
    case '42P07':
        /* JSSTYLED */
        msg = /.*relation "(\w+)_entry.*"/.exec(e.message);
        err = new BucketAlreadyExistsError(msg ? msg[1] : '');
        break;
    default:
        err = new InternalError(e.message);
        break;
    }
    err.cause = e.stack;
    return err;
}


function entryTable(bucket) {
    assert.ok(bucket);
    return bucket + '_entry';
}


function tombstoneTable(bucket) {
    assert.ok(bucket);
    return bucket + '_tombstone';
}


function indexObject(bucket, indexes, object) {
    assert.ok(bucket);
    assert.ok(indexes);
    assert.ok(object);

    var ndx = {};

    if (indexes.length === 0)
        return ndx;

    function index(k, v, t) {
        if (v !== undefined && typeof (v) !== t)
            throw new IndexTypeError(bucket, k, t);

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

    Object.keys(indexes).forEach(function (k) {
        return index(k, object[k], indexes[k].type);
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
        return callback(pgError(err));
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

    this._buckets = {};
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
                    self.emit('error', pgError(error));
                });

                client.on('notice', function (message) {
                    self.log.trace('Postgres notice: %s',
                                   util.inspect(message));
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

        self._buckets = _buckets;
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
            console.log('%s:\t%j', k, buckets);
        });
    });
 </pre>
 *
 * @arg {function} callback - arguments will be 'err' and 'buckets'.
 * @throws {TypeError} if callback is not a function.
 */
DB.prototype.buckets = function buckets(callback) {
    assertArgument('callback', 'function', callback);

    var sql = 'SELECT DISTINCT table_name, C.column_name, C.data_type, ' +
        'CASE WHEN (C.column_name = CCU.column_name) THEN 1 ELSE 0 END ' +
        'AS is_unique FROM information_schema.columns AS C ' +
        'INNER JOIN information_schema.constraint_column_usage AS CCU '+
        'USING (table_name) WHERE table_name IN ' +
        '(SELECT table_name FROM information_schema.tables WHERE ' +
        ' table_name LIKE \'%_entry\' AND table_schema = \'public\')' +
        ' AND C.column_name NOT IN (\'key\', \'value\', \'etag\', \'mtime\')';

    var self = this;
    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        return waitForQuery(self.query(client, sql), function (err, rows) {
            self.pool.release(client);

            if (err)
                return callback(err);

            // The query above requires a little post processing, as unique
            // index columns will show up twice. Sigh.  If someone has a better
            // way to write that query, please do; I just don't really care.
            var obj = {};

            function getType(t) {
                switch (t) {
                case 'numeric':
                    return 'number';
                case 'boolean':
                    return 'boolean';
                default:
                    return 'string';
                }
            }

            rows.forEach(function (r) {
                var table = r.table_name.replace(/_entry$/, '');
                if (!obj[table])
                    obj[table] = {};

                if (!obj[table][r.column_name]) {
                    obj[table][r.column_name] = {
                        type: getType(r.data_type),
                        unique: false
                    };
                }

                if (r.is_unique)
                    obj[table][r.column_name].unique = true;
            });

            self.log.trace({buckets: obj}, 'Returning buckets');
            return callback(null, obj);
        });
    });
};


/**
 * Creates a new bucket.
 *
 <pre>
    var index = {
        amount: {
            type: 'number'
        },
        email: {
            type: 'string',
            unique: true
        }
    };
    db.createBucket('foo', ['name', 'email'], function (err) {
        assert.ifError(err);
        ...
    });
 </pre>
 *
 * @arg {string} bucket         - The name of the bucket to create (globally).
 * @arg {object} indexes        -  Attributes to index.
 * @arg {function} callback     - `function (err) {}`.
 * @throws {TypeError} if bucket is not a string.
 */
DB.prototype.createBucket = function createBucket(bucket, indexes, callback) {
    assertArgument('bucket', 'string', bucket);
    if (typeof (indexes) === 'function') {
        callback = indexes;
        indexes = {};
    }
    assertArgument('indexes', 'object', indexes);
    assertArgument('callback', 'function', callback);

    var client;
    var entry;
    var self = this;
    var tasks;
    var tombstone;

    // entry table
    entry = 'CREATE TABLE ' + entryTable(bucket) + ' (' +
        'key TEXT PRIMARY KEY, ' +
        'value TEXT NOT NULL, ' +
        'etag CHAR(32) NOT NULL, ';

    tombstone = 'CREATE TABLE ' + tombstoneTable(bucket) + ' (' +
        'key TEXT PRIMARY KEY, ' +
        'value TEXT NOT NULL, ' +
        'etag CHAR(32) NOT NULL, ';

    Object.keys(indexes).forEach(function (k) {
        entry += k + ' ';
        tombstone += k + ' ';
        switch (indexes[k].type) {
        case 'number':
            entry += 'NUMERIC';
            tombstone += 'NUMERIC';
            break;
        case 'boolean':
            entry += 'BOOLEAN';
            tombstone += 'BOOLEAN';
            break;
        default:
            entry += 'TEXT';
            tombstone += 'TEXT';
            break;
        }
        if (indexes[k].unique)
            entry += ' UNIQUE';
        entry += ', ';
        tombstone += ', ';
    });
    entry += 'mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)';
    tombstone += 'dtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)';

    tasks = [
        function begin(cb) {
            return waitForQuery(self.query(client, 'BEGIN'), cb);
        },

        function createEntry(cb) {
            return waitForQuery(self.query(client, entry), cb);
        },

        function createTombstone(cb) {
            return waitForQuery(self.query(client, tombstone), cb);
        }

    ];

    // Indexes
    Object.keys(indexes).forEach(function (k) {
        if (indexes[k].unique)
            return;

        tasks.push(function (cb) {
            var sql = 'CREATE INDEX ' +
                entryTable(bucket) + '_' + k + '_idx ON ' +
                entryTable(bucket) + '(' + k + ') WHERE ' +
                k + ' IS NOT NULL';
            return waitForQuery(self.query(client, sql), cb);
        });
    });
    tasks.push(function commit(cb) {
        return waitForQuery(self.query(client, 'COMMIT'), cb);
    });

    return this.pool.acquire(function (poolErr, dbClient) {
        if (poolErr)
            return callback(internalError(poolErr));

        client = dbClient;
        return async.series(tasks, function (err) {
            self.pool.release(client);

            if (err)
                return callback(err);

            self._buckets[bucket] = indexes || {};
            return callback(null, self._buckets[bucket]);
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

    var client;
    var tasks = [
        function begin(cb) {
            return waitForQuery(self.query(client, 'BEGIN'), cb);
        },
        function dropEntry(cb) {
            var sql = 'DROP TABLE IF EXISTS ' + entryTable(bucket);
            return waitForQuery(self.query(client, sql), cb);
        },
        function dropTombstone(cb) {
            var sql =  'DROP TABLE IF EXISTS ' + tombstoneTable(bucket);
            return waitForQuery(self.query(client, sql), cb);
        },
        function commit(cb) {
            return waitForQuery(self.query(client, 'COMMIT'), cb);
        }
    ];
    var self = this;

    return this.pool.acquire(function (poolErr, dbClient) {
        if (poolErr)
            return callback(internalError(poolErr));

        client = dbClient;
        return async.series(tasks, function (err) {
            self.pool.release(client);

            if (err)
                return callback(err);

            delete self._buckets[bucket];
            return callback(null);
        });
    });
};


/**
 * Lists keys under a bucket, optionally with a prefix.
 *
 * @arg {string} bucket         - name of the bucket.
 * @arg {object} options        - options object (optional).
 * @arg {number} options.limit  - number of keys to return (optional).
 * @arg {number} options.offset - the usual offset marker (optional).
 * @arg {string} options.prefix - keys to start listing on (optional).
 * @arg {function} callback     - of the form function(err, query).
 */
DB.prototype.list = function list(bucket, options, callback) {
    assertArgument('bucket', 'string', bucket);
    if (typeof (options) === 'function') {
        callback = options;
        options = {};
    }
    assertArgument('options', 'object', options);
    assertArgument('callback', 'function', callback);

    var client;
    var keys = { total: 0 };
    var limit = options.limit || 1000;
    var offset = options.offset || 0;
    var self = this;

    var tasks = [
        function begin(cb) {
            return waitForQuery(self.query(client, 'BEGIN'), cb);
        },

        function count(cb) {
            var sql = 'SELECT COUNT(key) FROM ' + entryTable(bucket);
            if (options.prefix)
                sql += ' WHERE key LIKE \'' + options.prefix + '%\'';

            return waitForQuery(self.query(client, sql), function (err, rows) {
                if (err)
                    return cb(err);

                keys.total = rows[0].count;
                return cb();
            });
        },

        function listKeys(cb) {
            var sql = 'SELECT key, etag, mtime, COUNT(*) OVER () FROM ' +
                entryTable(bucket);
            if (options.prefix)
                sql += ' WHERE key LIKE \'' + options.prefix + '%\'';
            sql += sprintf(' ORDER BY key LIMIT %d OFFSET %d', limit, offset);
            return waitForQuery(self.query(client, sql), function (err, rows) {
                if (err)
                    return cb(err);

                keys.keys = rows;
                return cb();
            });
        },

        function rollback(cb) {
            client.rollback(cb);
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

            return callback(null, keys);
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
DB.prototype.put = function put(bucket, key, value, etag, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assertArgument('value', 'object', value);
    if (typeof (etag) === 'function') {
        callback = etag;
        etag = {};
    }
    assertArgument('etag', 'object', etag);
    assertArgument('callback', 'function', callback);

    var client;
    var data = JSON.stringify(value);
    var newEtag = crypto.createHash('md5').update(data).digest('hex');
    var oldRows;
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

                if (rows.length === 0) {
                    skipTombstone = true;
                } else {
                    assert.equal(rows.length, 1);
                }

                oldRows = rows;
                return cb();
            });
        },

        function checkEtag(cb) {
            if (!etag.value)
                return cb();

            if (oldRows.length === 0)
                return cb(new EtagConflictError(bucket, key, 'does not exist'));

            var match = oldRows[0].etag === etag.value;
            if (!match !== !etag.match)
                return cb(new EtagConflictError(bucket, key, oldRows[0].etag));

            return cb();
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

            Object.keys(self._buckets[bucket]).forEach(function (i) {
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

            var indexes;
            try {
                indexes = indexObject(bucket, self._buckets[bucket], value);
            } catch (e) {
                return cb(e);
            }

            var keys = Object.keys(indexes);
            var vals = [key, data, newEtag];

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

            return callback(null, newEtag);
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
DB.prototype.get = function get(bucket, key, tombstone, callback) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    if (typeof (tombstone) === 'function') {
        callback = tombstone;
        tombstone = false;
    }
    assertArgument('tombstone', 'boolean', tombstone);
    assertArgument('callback', 'function', callback);

    var self = this;
    var sql = sprintf('SELECT key, value, etag, %s as mtime FROM %s ' +
                      'WHERE key=\'%s\'',
                      tombstone ? 'dtime' : 'mtime',
                      tombstone ? tombstoneTable(bucket) : entryTable(bucket),
                      key);

    return this.pool.acquire(function (poolErr, client) {
        if (poolErr)
            return callback(internalError(poolErr));

        return waitForQuery(self.query(client, sql), function (err, rows) {
            if (err) {
                self.pool.release(client);
                return callback(err);
            }

            // Check if we need to return a 410 (i.e., the key was tombstoned)
            if (rows.length === 0 && !tombstone) {
                var sql2 = sprintf('SELECT dtime FROM %s WHERE key=\'%s\'',
                                   tombstoneTable(bucket), key);
                var tq = self.query(client, sql2);
                return waitForQuery(tq, function (err2, rows2) {
                    self.pool.release(client);
                    if (err2)
                        return callback(err2);

                    if (rows2.length === 0)
                        return callback(new ObjectNotFoundError(bucket, key));

                    return callback(new ResourceGoneError(bucket,
                                                          key,
                                                          rows2[0].dtime));
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
                    return cb(err);
                } else if (rows.length === 0) {
                    r.rollback(function () {
                        return cb(new ObjectNotFoundError(bucket, key));
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

            Object.keys(self._buckets[bucket]).forEach(function (i) {
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

                if (rows.length === 0) {
                    return cb(new ObjectNotFoundError(bucket,
                                                      key + ':tombstone'));
                }

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
            var indexes = Object.keys(self._buckets[bucket]);
            var sql = sprintf('WITH moved_rows AS (\n' +
                              '    DELETE FROM %s\n' +
                              '    WHERE\n' +
                              '        key=\'%s\'\n' +
                              'RETURNING key, value, etag',
                              tombstoneTable(bucket), key);

            indexes.forEach(function (i) {
                sql += ', ' + i;
            });
            sql += sprintf(')\nINSERT INTO %s(key, value, etag',
                           entryTable(bucket));
            indexes.forEach(function (i) {
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

    var index = this._buckets[bucket];
    var self = this;
    var sql = 'SELECT key, value, etag, mtime FROM ' + entryTable(bucket) +
        ' WHERE ' + attribute;

    if (!index)
        return callback(new NotIndexedError(bucket, attribute));

    switch (index.type) {
    case 'number':
        try {
            sql += ' = ' + parseInt(value, 10);
        } catch (e) {
            return callback(new IndexTypeError(bucket, attribute, index.type));
        }
        break;
    case 'boolean':
    default:
        if (value.indexOf('*') === -1) {
            sql += '= \'' + value + '\'';
        } else {
            var esc = false;
            var wc = false;
            var val = '';
            for (var i = 0; i < value.length; i++) {
                if (esc) {
                    continue;
                } else {
                    if (value[i] === '\\') {
                        esc = true;
                    } else if (value[i] === '*') {
                        wc = true;
                        val += '%';
                    } else {
                        val += value[i];
                    }
                }
            }

            if (wc) {
                sql += ' LIKE \'' + val + '\'';
            } else {
                sql += ' = \'' + val + '\'';
            }
        }
        break;
    }


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
