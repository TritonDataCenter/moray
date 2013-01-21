// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var pg = require('pg').native;
var pooling = require('pooling');
var uuid = require('node-uuid');



///--- Globals

var slice = Array.prototype.slice;
var sprintf = util.format;

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



///--- Helpers

function _pgSetup(options) {
        assert.object(options, 'options');
        assert.object(options.client, 'options.client');
        assert.number(options.connectTimeout, 'options.connectTimeout');
        assert.number(options.firstResultTimeout, 'options.firstResultTimeout');
        assert.object(options.log, 'options.log');
        assert.object(options.pool, 'options.pool');
        assert.number(options.queryTimeout, 'options.queryTimeout');

        var client = options.client;
        var log = options.log;
        var pool = options.pool;

        if (++CLIENT_ID >= 4294967295) // 2^32 -1
                CLIENT_ID = 1;

        client._moray_id = CLIENT_ID;

        client.begin = function begin(opts, cb) {
                if (typeof (opts) === 'function') {
                        cb = opts;
                        opts = {};
                }
                assert.object(opts, 'options');
                assert.func(cb, 'callback');

                var q = client.query('BEGIN');

                q.once('error', cb);
                q.once('end', function () {
                        cb(null);
                });
        };

        client.commit = function commit(cb) {
                assert.func(cb, 'callback');

                log.trace({client: client}, 'commit: entered');

                function end(err) {
                        log.trace({
                                client: client,
                                err: err
                        }, 'commit: %s', err ? 'failed' : 'done');
                        client.release();
                        cb(err || null);
                }

                var q = client.query('COMMIT');
                q.once('error', end);
                q.once('end', end.bind(this, null));
        };

        client.release = pool.release.bind(pool, client);
        client.rollback = function rollback(cb) {
                assert.optionalFunc(cb, 'callback');

                log.trace({client: client}, 'rollback: entered');

                function end(err) {
                        log.trace({
                                client: client,
                                err: err
                        }, 'rollback: done');
                        client.release();
                        if (cb)
                                cb(null);
                }

                var q = client.query('ROLLBACK');
                q.once('error', end);
                q.once('end', end.bind(this, null));
        };

        // Overwrite query so we can have timeouts
        var _query = client.query.bind(client);
        client.query = function query(sql) {
                assert.string(sql, 'sql');
                if (typeof (arguments[arguments.length - 1]) === 'function')
                        throw new TypeError('query: callback style illegal');

                var done = false;
                var req;
                var res = new EventEmitter();
                var queryTimer;
                var startTimer;

                log.debug({sql: sql}, 'pg.query: entered');

                function cleanup() {
                        if (queryTimer)
                                clearTimeout(queryTimer);
                        if (startTimer)
                                clearTimeout(startTimer);

                        req.removeAllListeners('end');
                        req.removeAllListeners('error');
                        req.removeAllListeners('row');

                        res.removeAllListeners('end');
                        res.removeAllListeners('error');
                        res.removeAllListeners('row');

                        done = true;
                }

                function emit(event, arg) {

                        switch (event) {
                        case 'row':
                                log.debug({
                                        row: arg,
                                        sql: sql
                                }, 'query: row');
                                if (!done)
                                        res.emit('row', arg);
                                break;

                        case 'error':
                                log.debug({
                                        err: arg,
                                        sql: sql
                                }, 'query: error');
                                if (!done) {
                                        res.emit('error', arg);
                                        cleanup();
                                }
                                break;

                        case 'end':
                                log.debug({
                                        sql: sql
                                }, 'query: end');

                                if (!done) {
                                        res.emit('end', arg);
                                        cleanup();
                                }

                                break;

                        default:
                                log.warn('query: unknown event %s', event);
                                break;
                        }
                }

                req = _query.apply(client, arguments);
                req.once('error', emit.bind(this, 'error'));
                req.once('end', emit.bind(this, 'end'));
                req.on('row', emit.bind(this, 'row'));
                req.once('row', function () {
                        if (startTimer)
                                clearTimeout(startTimer);
                });

                startTimer = setTimeout(function () {
                        log.warn({sql: sql}, 'pg.query: timeout on first row');
                        pool.pool.remove(client);
                        emit('error', new QueryTimeoutError(sql));
                }, options.firstResultTimeout);

                queryTimer = setTimeout(function () {
                        log.warn({sql: sql}, 'pg.query: query timeout');
                        pool.pool.remove(client);
                        emit('error', new QueryTimeoutError(sql));
                }, options.queryTimeout);

                return (res);
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
                                firstResultTimeout: options.firstResultTimeout,
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

        this._whatami = 'PGClient';

        EventEmitter.call(this);

        this.checkInterval = options.checkInterval;
        this.connectTimeout = options.connectTimeout;
        this.firstResultTimeout = options.firstResultTimeout;
        this.log = options.log.child({
                component: 'pgpool',
                serializers: SERIALIZERS
        });
        this.maxConnections = options.maxConnections;
        this.maxIdleTime = options.maxIdleTime;
        this.queryTimeout = options.queryTimeout;

        var pgOpts = {
                connectTimeout: self.connectTimeout,
                firstResultTimeout: self.firstResultTimeout,
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
                log: self.log.child({
                        level: 'warn'
                }),
                name: 'postgres',
                check: pgCheck(pgOpts),
                create: pgCreate(pgOpts),
                destroy: pgDestroy(pgOpts)
        });

        this.url = options.url;

        function reEmit(event) {
                if (self.log.debug()) {
                        var args = slice(arguments);
                        args.unshift();
                        self.log.debug('pool event %s: %j', event, args);
                }

                self.emit.apply(self, arguments);
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
                self.removeAllListeners('death');
                self.removeAllListeners('drain');
                self.removeAllListeners('end');
                self.removeAllListeners('error');

                self._deadbeef = true;

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


PGClient.prototype.release = function release(client) {
        assert.object(client, 'client');

        this.pool.release(client);

        var self = this;
        this.log.trace({
                client: client,
                pool: self.pool
        }, 'release: done');
};


PGClient.prototype.toString = function toString() {
        var str = '[object PGClient <' +
                'checkInterval=' + this.checkInterval + ', ' +
                'maxConnections=' + this.maxConnections + ', ' +
                'maxIdleTime=' + this.maxIdleTime + ', ' +
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
                opts.firstResultTimeout = opts.firstResultTimeout || 2000;
                opts.maxConnections = opts.maxConnections || 5;
                opts.maxIdleTime = opts.maxIdleTime || 120000;
                opts.queryTimeout = opts.queryTimeout || 4000;
                return (new PGClient(opts));
        },

        Client: PGClient,

        ConnectTimeoutError: ConnectTimeoutError,

        QueryTimeoutError: QueryTimeoutError
};
