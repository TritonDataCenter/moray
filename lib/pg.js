// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var pg = require('pg');
var pooling = require('pooling');
var uuid = require('node-uuid');



///--- Globals

var slice = Array.prototype.slice;

var CLIENT_ID = 0;

var SERIALIZERS = {
        client: function _serializeClient(c) {
                return (c._moray_id);
        },
        pool: function _serializePool(p) {
                return ({
                        available: p.available.length,
                        max: p.max,
                        size: p.resources.length,
                        waiting: p.queue.length
                });
        },
        err: bunyan.stdSerializers.err
};



///-- Errors

function ConnectTimeoutError(t) {
        this.name = 'ConnectTimeoutError';
        this.message = 'connect timeout after: ' + t + 'ms';

        Error.captureStackTrace(this, ConnectTimeoutError);
}
util.inherits(ConnectTimeoutError, Error);


function QueryTimeoutError(sql) {
        this.name = 'QueryTimeoutError';
        this.message = 'query timeout: ' + sql;

        Error.captureStackTrace(this, QueryTimeoutError);
}
util.inherits(QueryTimeoutError, Error);



///--- Helpers

function _pgSetup(options) {
        assert.object(options, 'options');
        assert.object(options.client, 'options.client');
        assert.number(options.connectTimeout, 'options.connectTimeout');
        assert.object(options.log, 'options.log');
        assert.object(options.pool, 'options.pool');
        assert.number(options.queryTimeout, 'options.queryTimeout');

        var client = options.client;
        var _log = options.log;
        var pool = options.pool;

        if (++CLIENT_ID >= 4294967295) // 2^32 -1
                CLIENT_ID = 1;

        client._moray_id = CLIENT_ID;

        client.begin = function begin(cb) {
                assert.func(cb, 'callback');
                var q = client.query('BEGIN');
                q.once('error', cb);
                q.once('end', function () {
                        cb(null);
                });
        };

        client.commit = pool.commit.bind(pool, client);
        client.release = pool.release.bind(pool, client);
        client.rollback = pool.rollback.bind(pool, client);

        // Overwrite query so we can have timeouts
        var _query = client.query.bind(client);
        client.query = function query(sql) {
                var log = _log;
                if (_log.debug())
                        log = _log.child({id: uuid.v1().substr(0, 7)}, true);
                var argv = arguments;
                var done = false;
                var request;
                var queryTimer;
                var startTimer;

                function cleanup() {
                        if (queryTimer)
                                clearTimeout(queryTimer);
                        if (startTimer)
                                clearTimeout(startTimer);

                        request.removeListener('end', onError);
                        request.removeListener('error', onError);
                        request.removeAllListeners('row');
                }

                function cbErr(err, emit) {
                        if (done)
                                return;

                        cleanup();
                        done = true;
                        var _cb = slice.call(argv).pop();
                        if (typeof (_cb) !== 'function') {
                                if (emit)
                                        request.emit('error', err);
                        } else {
                                _cb(err);
                        }
                }

                function onEnd() {
                        log.debug('query: done');
                        cleanup();
                }

                function onError(err) {
                        log.debug(err, 'query: error');
                        cbErr(err, false);
                }

                log.debug({sql: sql}, 'pg.query: entered');

                request = _query.apply(client, arguments);
                request.once('error', onError);
                request.once('end', onEnd);
                request.once('row', function () {
                        if (startTimer)
                                clearTimeout(startTimer);
                });

                startTimer = setTimeout(function () {
                        log.warn({sql: sql}, 'pg.query: ACK timeout');
                        pool.pool.remove(client);
                        cbErr(new QueryTimeoutError(sql), true);
                }, options.connectTimeout);

                queryTimer = setTimeout(function () {
                        log.warn({sql: sql}, 'pg.query: query timeout');
                        cbErr(new QueryTimeoutError(sql), true);
                }, options.queryTimeout);

                return (request);
        };
}


function pgAssert(_pg) {
        assert.ok(_pg, 'pg handle');
        assert.ok(_pg.connection, 'pg connection');
        assert.ok(_pg.connection.stream, 'pg stream');
        assert.ok(_pg.connection.stream.readable, 'pg readable');
        assert.ok(!_pg.connection.stream.destroyed, 'pg not destroyed');
        assert.ok(_pg.connection.stream.writable, 'pg writable');
        return (true);
}


function pgCheck(options) {
        function _pgCheck(client, cb) {
                var ok = false;
                var req = client.query('SELECT NOW() AS when');
                req.once('error', cb);
                req.once('row', function (row) {
                        ok = true;
                });
                req.once('end', function () {
                        if (!ok) {
                                cb(new Error('no rows received'));
                        } else {
                                cb();
                        }
                });
        }

        return (_pgCheck);
}


function pgCreate(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.pool, 'options.pool');
        assert.string(options.url, 'options.url');

        var log = options.log.child({component: 'PGClient'}, true);

        function _pgCreate(cb) {
                var client = new pg.Client(options.url);
                var done = false;
                var timer = setTimeout(function () {
                        client.removeAllListeners('connect');
                        // do not remove error listener as node will still throw
                        _cb(new ConnectTimeoutError(options.connectTimeout));
                }, options.connectTimeout);

                function _cb(err, _pg) {
                        if (done)
                                return;

                        done = true;
                        cb(err, _pg);
                }

                client.once('connect', function onConnect() {
                        if (timer)
                                clearTimeout(timer);

                        client.removeAllListeners('error');

                        _pgSetup({
                                client: client,
                                connectTimeout: options.connectTimeout,
                                log: options.log,
                                pool: options.pool,
                                queryTimeout: options.queryTimeout,
                                url: options.url
                        });

                        _cb(null, client);
                });

                client.once('error', function onError(err) {
                        log.fatal(err, 'unable to create PG client');

                        if (timer)
                                clearTimeout(timer);
                        client.removeAllListeners('connect');
                        _cb(err);
                });

                client.connect();
        }

        return (_pgCreate);
}


function pgDestroy(options) {
        function _pgDestroy(client) {
                client.removeAllListeners('end');
                client.removeAllListeners('error');
                client.removeAllListeners('row');
                client.end();
        }

        return (_pgDestroy);
}



///--- API

/**
 * Creates a new (pooled) Postgres client.
 *
 * var db = new PGClient({
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
function PGClient(options) {
        assert.object(options, 'options');
        assert.number(options.checkInterval, 'options.checkInterval');
        assert.number(options.connectTimeout, 'options.connectTimeout');
        assert.object(options.log, 'options.log');
        assert.number(options.maxConnections, 'options.maxConnections');
        assert.number(options.maxIdleTime, 'options.maxIdleTime');
        assert.number(options.queryTimeout, 'options.queryTimeout');

        var self = this;

        EventEmitter.call(this);

        this.checkInterval = options.checkInterval;
        this.connectTimeout = options.connectTimeout;
        this.log = options.log.child({
                component: 'pgpool',
                role: options.role,
                serializers: SERIALIZERS
        });
        this.maxConnections = options.maxConnections;
        this.maxIdleTime = options.maxIdleTime;
        this.queryTimeout = options.queryTimeout;

        var pgOpts = {
                connectTimeout: self.connectTimeout,
                log: self.log,
                pool: self,
                queryTimeout: self.queryTimeout,
                url: options.url
        };
        this.pool = pooling.createPool({
                assert: pgAssert,
                checkInterval: options.checkInterval,
                max: options.maxConnections,
                maxIdleTime: options.maxIdleTime,
                log: options.log,
                name: 'postgres',

                check: pgCheck(pgOpts),
                create: pgCreate(pgOpts),
                destroy: pgDestroy(pgOpts)
        });

        this.role = options.role;
        this.url = options.url;

        function reEmit(event) {
                if (this.log.debug()) {
                        var args = slice(arguments);
                        args.unshift();
                        this.log.debug('pool event %s: %j', event, args);
                }

                this.emit.apply(this, arguments);
        }

        this.pool.on('death', reEmit.bind(this, 'death'));
        this.pool.on('drain', reEmit.bind(this, 'drain'));
        this.pool.once('end', reEmit.bind(this, 'end'));
        this.pool.once('error', reEmit.bind(this, 'error'));
}
util.inherits(PGClient, EventEmitter);


PGClient.prototype.close = function close(cb) {
        var self = this;

        this.log.trace({pool: self.pool}, 'close: entered');
        this.pool.shutdown(function () {
                self.log.trace({pool: self.pool}, 'close: closed');
                if (typeof (cb) === 'function')
                        cb();

                self.emit('close');
        });
};


PGClient.prototype.checkout = function checkout(callback) {
        assert.func(callback, 'callback');

        var log = this.log;
        var self = this;

        log.trace({pool: self.pool}, 'checkout: entered');
        this.pool.acquire(function (err, client) {
                if (err) {
                        log.trace(err, 'checkout: failed');
                        callback(err);
                } else {
                        log.trace({
                                client: client
                        }, 'checkout: done');
                        callback(null, client);
                }
        });
};


PGClient.prototype.commit = function commit(client, callback) {
        assert.object(client, 'client');
        assert.func(callback, 'callback');

        var log = this.log;
        var self = this;
        log.trace({client: client}, 'commit: entered');

        client.query('COMMIT', function (err) {
                log.trace({
                        client: client,
                        err: err
                }, 'commit: %s', err ? 'failed' : 'done');
                self.pool.release(client);
                callback(err || null);
        });
};


PGClient.prototype.release = function release(client) {
        assert.object(client, 'client');

        this.pool.release(client);

        var self = this;
        this.log.trace({
                client: client,
                pool: self.pool
        }, 'release: done');
};


PGClient.prototype.rollback = function rollback(client, callback) {
        assert.object(client, 'client');
        assert.optionalFunc(callback, 'callback');

        var log = this.log;
        var self = this;

        log.trace({client: client}, 'rollback: entered');
        client.query('ROLLBACK', function (err) {
                log.trace({
                        client: client,
                        err: err
                }, 'rollback: done');
                self.release(client);
                if (callback)
                        callback(null);
        });
};


/**
 * Starts a transaction, and checks out a client from the pool.
 *
 * You are expected to call either `rollback` or `commit`.
 *
 * @arg {Function} callback of the form `function (err, client)`.
 */
PGClient.prototype.start = function start(callback) {
        assert.func(callback, 'callback');

        var log = this.log;
        var self = this;

        log.trace('start: entered');

        this.checkout(function (checkoutErr, client) {
                if (checkoutErr) {
                        log.trace(checkoutErr, 'start: checkout failed');
                        callback(checkoutErr);
                } else {
                        log.trace({client: client}, 'start: issuing BEGIN');
                        client.query('BEGIN', function (err) {
                                if (err) {
                                        log.trace({
                                                client: client,
                                                err: err
                                        }, 'start: BEGIN failed');
                                        self.pool.remove(client);
                                        callback(err);
                                } else {
                                        log.trace({
                                                client: client
                                        }, 'start: BEGIN done');
                                        callback(null, client);
                                }
                        });
                }
        });
};


PGClient.prototype.toString = function toString() {
        var str = '[object PGClient <' +
                'checkInterval=' + this.checkInterval + ', ' +
                'maxConnections=' + this.maxConnections + ', ' +
                'maxIdleTime=' + this.maxIdleTime + ', ' +
                (this.role ? ('role=' + this.role + ', ') : '') +
                'url=' + this.url + '>]';

        return (str);
};



///--- Exports

module.exports = {

        createClient: function createClient(options) {
                var opts = clone(options);
                opts.log = options.log;
                opts.checkInterval = opts.checkInterval || 60000;
                opts.connectTimeout = opts.connectTimeout || 1000;
                opts.maxConnections = opts.maxConnections || 5;
                opts.maxIdleTime = opts.maxIdleTime || 120000;
                opts.queryTimeout = opts.queryTimeout || 1000;
                return (new PGClient(opts));
        },

        Client: PGClient,

        ConnectTimeoutError: ConnectTimeoutError,

        QueryTimeoutError: QueryTimeoutError
};



///--- Tests
//
// Kill a DB a few times while running this; you should get some brief
// "connection/bad client/..." errors, followed by a purge of the resources
// in the pool, followed by reestablishment.
//
// var bunyan = require('bunyan');

// function newPool() {
//         return new PGClient({
//                 log: bunyan.createLogger({
//                         name: 'postgres',
//                         level: 'debug',
//                         stream: process.stdout
//                 }),
//                 connectTimeout: 500,
//                 checkInterval: 5000,
//                 maxConnections: 5,
//                 maxIdleTime: 1000,
//                 queryTimeout: 1000,
//                 url: 'pg://postgres@127.0.0.1:5432/postgres'
//         });
// }

// var pool = newPool();

// function ifError(f, err) {
//         if (err) {
//                 console.log(f + ': ' + err.stack);
//                 return true;
//         }
//         return false;
// }


// function onStart(err, pg) {
//         if (ifError('onStart', err)) {
//                 setTimeout(pool.start.bind(pool,onStart), 900);
//                 return;
//         }

//         pg.query('SELECT NOW() AS my_when', function (err) {
//                 ifError('query', err);

//                 pg.rollback(function (err2) {
//                         ifError('rollback', err2);

//                         setTimeout(pool.start.bind(pool,onStart), 900);
//                 });
//         });
// }

// for (var i = 0; i < 8; i++)
//         pool.start(onStart);

// setInterval(function dump() {
//         console.log('\n----');
//         console.log(new Date().toString());
//         console.log('pool: ' + JSON.stringify({
//                 a: pool.pool.available.length,
//                 q: pool.pool.queue.length,
//                 r: pool.pool.resources.length,
//         }, null, 4));

//         pool.pool.resources.map(function (c) {
//                 return (c.client);
//         }).forEach(function (pg) {
//                 console.log(pg._moray_id + ' -> ' +
//                             pg.connection.stream.destroyed);
//         });

//         console.log('----\n');

// }, 1000);
