/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var libuuid = require('libuuid');
var once = require('once');
var pg = require('pg');
require('pg-parse-float')(pg);
var pooling = require('pooling');

var dtrace = require('./dtrace');


///--- Globals

var slice = Array.prototype.slice;
var sprintf = util.format;

var CLIENT_ID = 0;

var SERIALIZERS = {
    client: function _serializeClient(c) {
        return ({
            id: c._moray_id,
            currentQuery: (c.activeQuery || {}).text,
            timeout: c._queryTimeout,
            txn: c._moray_txn
        });
    },
    err: bunyan.stdSerializers.err
};



///--- Postgres Query Wrapper

function qCleanup(req, res) {
    if (req) {
        req.removeAllListeners('end');
        req.removeAllListeners('error');
        req.removeAllListeners('row');
    }

    if (res) {
        res.removeAllListeners('end');
        res.removeAllListeners('error');
        res.removeAllListeners('row');
    }
}


// This is a "bound" function, so 'this' is always set to the current
// PG connection
function query(sql, args) {
    assert.string(sql, 'sql');
    if (typeof (arguments[arguments.length - 1]) === 'function')
        throw new TypeError('query: callback style illegal');

    // Clean up whitespace so queries are normalized to DTrace
    sql = sql.replace(/(\r\n|\n|\r)/gm, '').replace(/\s+/, ' ');

    var done;
    var log = this.log;
    var req;
    var res = new EventEmitter();
    var self = this;
    var timer;
    var reqid;

    done = once(function endOrError(event, arg) {
        if (log.debug()) {
            var lprops = {
                client: self,
                event: event
            };
            if (arg instanceof Error) {
                lprops.err = arg;
            } else {
                lprops.res = arg;
            }
            log.debug('query: done');
        }

        res.emit(event, arg);

        clearTimeout(timer);
        qCleanup(req, res);
    });

    req = this._query.apply(this, arguments);

    req.on('row', function onRow(row) {
        if (reqid === undefined)
            reqid = libuuid.create();
        dtrace['query-row'].fire(function () {
            return ([reqid, sql, row]);
        });
        log.debug({client: self, row: row}, 'query: row');

        clearTimeout(timer);
        res.emit('row', row);
    });

    req.on('end', function onQueryEnd(arg) {
        if (reqid === undefined)
            reqid = libuuid.create();
        dtrace['query-done'].fire(function () {
            return ([reqid, sql, arg]);
        });

        done('end', arg);
    });

    req.on('error', function onQueryError(err) {
        if (reqid === undefined)
            reqid = libuuid.create();
        dtrace['query-error'].fire(function () {
            return ([reqid, sql, err.toString()]);
        });

        done('error', err);
    });

    if (this._queryTimeout > 0) {
        timer = setTimeout(function onRowTimeout() {
            // Don't rollback, since it may or may not
            // make it to the server; just force a connection
            // close to happen once the conn is out of the
            // pool
            self._moray_had_err = true;
            done('error', new QueryTimeoutError(sql));
        }, this._queryTimeout);
    }

    dtrace['query-start'].fire(function () {
        if (reqid === undefined)
        	reqid = libuuid.create();
        return ([reqid, sql]);
    });
    log.debug({
        client: self,
        sql: sql,
        args: args
    }, 'pg.query: started');

    return (res);
}

///--- End Query API



///--- Pool Functions

function pgSetup(options) {
    assert.object(options, 'options');
    assert.object(options.client, 'options.client');
    assert.object(options.log, 'options.log');
    assert.object(options.pool, 'options.pool');
    assert.number(options.queryTimeout, 'options.queryTimeout');

    var client = options.client;
    var log = options.log;
    var pool = options.pool;

    if (++CLIENT_ID >= 4294967295) // 2^32 -1
        CLIENT_ID = 1;

    client._moray_id = CLIENT_ID;
    client._queryTimeout = options.queryTimeout;
    client._moray_txn = false;

    client.connection.stream.setKeepAlive(true);
    client.log = log.child({
        component: 'PGClient',
        moray_id: client._moray_id
    }, true);
    client.release = pool.release.bind(pool, client);

    // Overwrite query so we can have timeouts, DTrace, etc.
    client._query = client.query.bind(client);
    client.query = query.bind(client);

    // Some friendly wrappers
    client.begin = function begin(level, cb) {
        if (typeof (level) === 'function') {
            cb = level;
            level = 'READ COMMITTED';
        }
        assert.func(cb, 'callback');
        cb = once(cb);

        var q = client.query('BEGIN TRANSACTION ISOLATION LEVEL ' + level);
        q.once('error', function (err) {
            client._moray_had_err = true;
            pool.release(client);
            cb(err);
        });
        q.once('end', function () {
            client._moray_txn = true;
            cb();
        });
    };
    client.commit = function commit(cb) {
        assert.func(cb, 'callback');
        cb = once(cb);

        function _cb(err) {
            client._moray_txn = false;
            pool.release(client);
            cb(err);
        }

        var q = client.query('COMMIT');
        q.once('error', _cb);
        q.once('end', function () {
            _cb();
        });
    };

    client.rollback = function rollback(cb) {
        assert.optionalFunc(cb, 'callback');
        cb = once(cb || function () {});

        function _cb(err) {
            client._moray_txn = false;
            pool.release(client);
            cb(err);
        }

        if (client._moray_txn) {
            var q = client.query('ROLLBACK');
            q.once('error', function (err) {
                client._moray_had_err = true;
                _cb(err);
            });
            q.once('end', function () {
                _cb();
            });
        } else {
            _cb();
        }
    };
}


function pgAssert(_pg) {
    assert.ok(_pg, 'pg handle');
    assert.ok(_pg.connection, 'pg connection');
    assert.ok(_pg.connection.stream, 'pg stream');
    assert.ok(_pg.connection.stream.readable, 'pg readable');
    assert.ok(!_pg.connection.stream.destroyed, 'pg not destroyed');
    assert.ok(_pg.connection.stream.writable, 'pg writable');

    return (!_pg._moray_had_err);
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


function pgCreate(opts) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.pool, 'options.pool');
    assert.string(opts.url, 'options.url');

    var log = opts.log;

    function _pgCreate(cb) {

        cb = once(cb);

        var client = new pg.Client(opts.url);
        if (opts.connectTimeout > 0) {
            var timer = setTimeout(function () {
                // do not remove error listener as node may
                // still throw
                client.removeAllListeners('connect');
                if (client.connection &&
                    client.connection.stream) {
                    client.connection.stream.destroy();
                }

                var t = opts.connectTimeout;
                cb(new ConnectTimeoutError(t));
            }, opts.connectTimeout);
        }

        client.once('connect', function onConnect() {
            clearTimeout(timer);

            client.removeAllListeners('error');

            pgSetup({
                client: client,
                connectTimeout: opts.connectTimeout,
                log: opts.log,
                pool: opts.pool,
                queryTimeout: opts.queryTimeout,
                url: opts.url
            });

            cb(null, client);
        });

        client.once('error', function onError(err) {
            log.fatal(err, 'unable to create PG client');

            clearTimeout(timer);
            client.removeAllListeners('connect');

            if (client.connection && client.connection.stream)
                client.connection.stream.destroy();

            cb(err);
        });

        client.connect();
    }

    return (_pgCreate);
}


function pgDestroy(opts) {
    var log = opts.log;

    function _pgDestroy(client) {
        log.warn({client: client}, 'pg: destroying connection');

        client.removeAllListeners('connect');
        client.removeAllListeners('end');
        client.removeAllListeners('error');
        client.removeAllListeners('row');

        if (client.connection && client.connection.stream)
            client.connection.stream.destroy();

        client._deadbeef = true;
    }

    return (_pgDestroy);
}

///--- End Pool Functions



///--- API

/**
 * Creates a new (pooled) Postgres client.
 *
 * var db = new PGPool({
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
function PGPool(options) {
    assert.object(options, 'options');
    assert.number(options.checkInterval, 'options.checkInterval');
    assert.number(options.connectTimeout, 'options.connectTimeout');
    assert.object(options.log, 'options.log');
    assert.number(options.maxConnections, 'options.maxConnections');
    assert.number(options.maxIdleTime, 'options.maxIdleTime');
    assert.number(options.queryTimeout, 'options.queryTimeout');

    var self = this;

    this._whatami = 'PGPool';

    EventEmitter.call(this);

    this.checkInterval = options.checkInterval;
    this.connectTimeout = options.connectTimeout;
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
        log: self.log,
        name: 'pgpool' + (options.role ? '-' + options.role : ''),
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
util.inherits(PGPool, EventEmitter);


PGPool.prototype.close = function close(cb) {
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


PGPool.prototype.checkout = function checkout(callback) {
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

            // MANTA-1458 - hack!
            client._defaultTimeout = client._queryTimeout;
            client.setTimeout = function setTimeout(t) {
                client._queryTimeout = t;
            };
            callback(null, client);
        }
    });
};


PGPool.prototype.release = function release(client) {
    assert.object(client, 'client');

    // MANTA-1458 - unhack!
    client._queryTimeout = client._defaultTimeout;
    this.pool.release(client);

    var self = this;
    this.log.trace({
        client: client,
        pool: self.pool
    }, 'release: done');
};


PGPool.prototype.toString = function toString() {
    var str = '[object PGPool <' +
        'checkInterval=' + this.checkInterval + ', ' +
        'maxConnections=' + this.maxConnections + ', ' +
        'maxIdleTime=' + this.maxIdleTime + ', ' +
        'url=' + this.url + '>]';

    return (str);
};



///--- Exports

module.exports = {

    createPool: function createPool(options) {
        assert.object(options, 'options');
        var l = options.log;
        if (options.log)
            delete options.log;

        var opts = clone(options);
        options.log = l;
        opts.log = l;

        function number(param, def) {
            if (opts[param] === undefined) {
                opts[param] = def;
            } else if (opts[param] === false) {
                opts[param] = 0;
            }
            // else noop
        }

        number('checkInterval', 60000);
        number('connectTimeout', 1000);
        number('maxIdleTime', 120000);
        number('maxConnections', 5);
        number('queryTimeout', 30000);

        return (new PGPool(opts));
    },

    PGPool: PGPool,

    ConnectTimeoutError: ConnectTimeoutError,

    QueryTimeoutError: QueryTimeoutError
};
