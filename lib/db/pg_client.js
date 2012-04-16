// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var async = require('async');
var Logger = require('bunyan');
var poolModule = require('generic-pool');
var pg = require('pg');
var uuid = require('node-uuid');

var args = require('../args');
var errors = require('../errors');



///--- Globals

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

    switch (e.code) {
    case '22P02':
        err = new InvalidArgumentError(e.message);
        break;
    case '23505':
        /* JSSTYLED */
        msg = /.*violates unique constraint.*/.test(e.message);
        if (msg) {
            // This PG errror looks like:
            // Key (email)=(foo@joyent.com) already exists
            msg = /\((.*)\)=\((.*)\)/.exec(e.detail);
            err = new UniqueAttributeError(msg[1], msg[2]);
        } else {
            err = new InvalidArgumentError(e.detail);
        }
        break;
    case '42601':
    case '42701':
        err = new InternalError('Invalid (moray) SQL: %s', e.message);
        break;
    case '42P01':
        /* JSSTYLED */
        msg = /.*relation "(\w+)_entry".*/.exec(e.message);
        err = new BucketNotFoundError(msg ? msg[1] : '');
        break;
    case '42P07':
        /* JSSTYLED */
        msg = /.*relation "(\w+)_entry.*"/.exec(e.message);
        err = new BucketAlreadyExistsError(msg ? msg[1] : '');
        break;
    default:
        console.error('pgError: untranslated exception: %s', util.inspect(e));
        err = new InternalError(e.message);
        break;
    }
    err.cause = e.stack;
    return err;
}



///--- Postgres Driver API

/**
 * Creates a new (pooled) Postgres client.
 *
 * var db = new PostgresClient({
 *     url: 'pg://unit:test@localhost/test',
 *     log: new Logger({
 *         stream: process.stderr,
 *         serializers: Logger.stdSerializers
 *      }),
 *      maxConns: 100,
 *      idleTimeout: 60000
 * });
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
 * @fires 'connect' - `db.on('connect', function () {});`
 * @fires 'error'   - `db.on('error', function (err {});`
 *
 * @throws {TypeError} on bad input types.
 */
function Postgres(options) {
    assertObject('options', options);
    assertObject('options.log', options.log);
    assertString('options.url', options.url);

    var self = this;
    EventEmitter.call(this);

    this.log = options.log;
    this.url = options.url;

    this.log.queryId = function queryId() {
        return uuid().substr(0, 7);
    };

    this.pool = poolModule.Pool({
        name: 'postgres',
        create: function create(callback) {
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

                client.commit = function pgCommit(_release, cb) {
                    if (typeof (_release) === 'function') {
                        cb = _release;
                        _release = true;
                    }
                    assertFunction('callback', cb);

                    self.log.debug('Executing COMMIT');
                    return client.query('COMMIT', function (commitError) {
                        if (commitError)
                            return cb(commitError);

                        if (_release)
                            self.release(client);

                        return cb(null);
                    });
                };

                // Replace the Postgres Query with our own form that
                // auto-waits for rows and does a few other wraps.
                client._query = client.query;
                client.query = function pgQuery(sql, values, cb) {
                    if (typeof (values) === 'function') {
                        cb = values;
                        values = undefined;
                    }
                    return self._query(client, sql, values, cb);
                };

                client.release = function pgRelease() {
                    return self.release(client);
                };

                client.rollback = function pgRollback(_release, cb) {
                    if (typeof (_release) === 'function') {
                        cb = _release;
                        _release = true;
                    }
                    assertFunction('callback', cb);

                    self.log.debug('Executing ROLLBACK');
                    return client.query('ROLLBACK', function () {
                        if (_release)
                            self.release(client);

                        return cb();
                    });
                };

                return callback(null, client);
            });

            client.on('error', function (err) {
                self.log.error({err: err}, 'Postgres client error');
                self.pool.destroy(client);
            });
        },
        destroy: function destroy(client) {
            client.removeAllListeners('error');
            client.end();
        },
        max: options.maxConns || 100,
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
}
util.inherits(Postgres, EventEmitter);
module.exports = Postgres;


Postgres.prototype.checkout = function checkout(callback) {
    assertFunction('callback', callback);

    var self = this;
    if (this.log.debug()) {
        this.log.debug({
            poolSize: self.pool.getPoolSize(),
            availableConnections: self.pool.availableObjectsCount(),
            waitingClients: self.pool.waitingClientsCount()
        }, 'checkout called');
    }
    return this.pool.acquire(function (err, client) {
        self.log.debug('checkout done: %s', err ? err.stack : '');
        if (err)
            return callback(internalError(err));

        return callback(null, client);
    });
};


Postgres.prototype.release = function release(client) {
    assertObject('client', client);

    var self = this;
    if (this.log.debug()) {
        this.log.debug({
            poolSize: self.pool.getPoolSize(),
            availableConnections: self.pool.availableObjectsCount(),
            waitingClients: self.pool.waitingClientsCount()
        }, 'release called');

    }

    this.pool.release(client);
};


/**
 * Starts a transaction, and checks out a client from the pool.
 *
 * You are expected to call either `rollback` or `commit` on the returned
 * client object if using this API.
 *
 * @arg {Function} callback of the form `function (err, client)`.
 */
Postgres.prototype.start = function start(client, callback) {
    if (typeof (client) === 'function') {
        callback = client;
        client = null;
    }
    assertFunction('callback', callback);

    var self = this;

    function _start(_client) {
        return self._query(_client, 'BEGIN', function (err) {
            if (err)
                return callback(err);

            return callback(null, _client);
        });
    }

    if (client)
        return _start(client);

    return this.checkout(function (err, _client) {
        if (err)
            return callback(internalError(err));

        return _start(_client);
    });
};


/**
 * Useful for running a "one query" job.  This method does the equivalent of:
 *
 * postgres.checkout(function (err, client) {
 *     assert.ifError(err);
 *     client.query(sql, values, function (err, rows) {
 *         client.release();
 *         assert.ifError(err);
 *         return callback(null, rows);
 *     });
 * });
 *
 * @arg {String} sql        - SQL to execute.
 * @arg {Array} values      - (optional) values to match INSERT statements.
 * @arg {Function} callback - function (err, rows).
 */
Postgres.prototype.query = function query(sql, values, callback) {
    assertString('sql', sql);
    if (typeof (values) === 'function') {
        callback = values;
        values = undefined;
    }

    var self = this;
    return this.checkout(function (poolErr, client) {
        if (poolErr)
            return callback(poolErr);

        return self._query(client, sql, values, function (err, rows) {
            client.release();

            if (err)
                return callback(err);

            return callback(null, rows);
        });
    });
};


/**
 * Low-level Query API over Postgres.
 *
 * This returns an EventEmitter; if you want to wait for the rows to come
 * back, use `client.query(sql, values, cb)`, where client is what you got
 * from `Postgres.start()`.
 *
 * @arg {Object} conn  - a client that came from Postgres.start().
 * @arg {String} sql   - SQL to execute.
 * @arg {Array} values - (optional) values to match INSERT statements.
 * @arg {Functio} callback - function (err, rows).
 * @return {EventEmitter}
 */
Postgres.prototype._query = function _query(conn, sql, values, callback) {
    assertObject('conn', conn);
    assertString('sql', sql);
    if (typeof (values) === 'function') {
        callback = values;
        values = undefined;
    }
    if (values !== undefined)
        assertArray('values', null, values);
    assertFunction('callback', callback);

    var done = false;
    var log = this.log;
    var qid = this.log.queryId();
    var req;
    var rows = [];

    log.debug({
        query_id: qid,
        sql: sql,
        values: values || null
    }, 'query starting');

    req = conn._query(sql, values);

    req.on('row', function (row) {
        log.debug({row: row, query_id: qid}, 'row received');
        rows.push(row);
    });

    req.on('error', function (err) {
        log.debug({err: err, query_id: qid}, 'query error');
        if (!done) {
            done = true;
            return callback(pgError(err));
        }
        return false;
    });

    req.on('end', function () {
        log.debug({query_id: qid}, 'query complete');
        if (!done) {
            done = true;
            return callback(null, rows);
        }
        return false;
    });

    return req;
};


/**
 * Shuts down the underlying postgres connection(s).
 *
 * db.end(function (err) {
 *   assert.ifError(err);
 * });
 *
 * @arg {function} callback - only an error argument.
 */
Postgres.prototype.shutdown = function shutdown(callback) {
    this.log.debug('shutdown called');

    var self = this;
    return this.pool.drain(function () {
        self.pool.destroyAllNow();
        return (typeof (callback) === 'function' ? callback() : false);
    });
};
