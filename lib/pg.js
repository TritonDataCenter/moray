/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var pg = require('pg');
require('pg-parse-float')(pg);

var mod_cueball = require('cueball');
var VError = require('verror');

var dtrace = require('./dtrace');

var mod_errors = require('./errors');
var ConnectTimeoutError = mod_errors.ConnectTimeoutError;
var InternalError = mod_errors.InternalError;
var InvalidIndexDefinitionError = mod_errors.InvalidIndexDefinitionError;
var NoDatabasePeersError = mod_errors.NoDatabasePeersError;
var QueryTimeoutError = mod_errors.QueryTimeoutError;
var UniqueAttributeError = mod_errors.UniqueAttributeError;

var parsePgDate = pg.types.getTypeParser(1082, 'text');

var TYPES = require('./types').TYPES;

// --- Globals

var CLIENT_ID = 0;
var DBNAME = process.env.MORAY_DB_NAME || 'moray';

var SERIALIZERS = {
    pool: function _serializePool(p) {
        try {
            var s = p.getStats();
            return s;
        } catch (_) {
            return undefined;
        }
    },
    client: function _serializeClient(c) {
        return ({
            id: c._moray_id,
            currentQuery: jsprim.pluck(c, 'client.activeQuery.text'),
            timeout: c._queryTimeout,
            txn: c._moray_txn
        });
    },
    err: bunyan.stdSerializers.err
};

var TSTZRANGE_OID = 3910;
var TSTZRANGE_ARRAY_OID = 3911;

var TSTZRANGE_PG_RE =
    /^([([])(?:"([ \d:.TZ+-]+)")?,(?:"([ \d:.TZ+-]+)")?([)\]])$/;

// --- Internal helpers

/*
 * This is an extremely unfortunate interface that we need to maintain for
 * backwards compatibility. Moray's old pooling logic used to return the
 * node.js connection error to the consumer directly, without translating it
 * into something easier to check like "NoDatabasePeersError".
 *
 * To mimic the old behaviour, when we fail to claim a connection, we return an
 * error with the name "Error", and the message "connect ECONNREFUSED".
 * Services like SAPI and Marlin rely on this as part of checking whether Moray
 * is effectively down.
 */
function createNoBackendError() {
    return new Error('connect ECONNREFUSED');
}

function createOverloadedError() {
    return new NoDatabasePeersError(
        'unable to acquire backend connection due to service being overloaded',
        'OverloadedError', 'maximum moray queue length reached');
}

function fixupDateRange(value) {
    if (value === null) {
        return null;
    }

    var lower = '';
    var upper = '';

    var m = TSTZRANGE_PG_RE.exec(value);
    if (m === null) {
        throw new VError('invalid date range: %j', value);
    }

    if (m[2]) {
        lower = parsePgDate(m[2]).toISOString();
    }

    if (m[3]) {
        upper = parsePgDate(m[3]).toISOString();
    }

    return m[1] + lower + ',' + upper + m[4];
}


// --- Postgres Client Wrapper Class

/*
 * This client wraps the node-postgres client to provide some useful helper
 * methods, and also to provide timeouts and DTrace probes inside the query
 * function.
 */
function PGClient(options) {
    assert.object(options, 'options');
    assert.object(options.client, 'options.client');
    assert.object(options.log, 'options.log');
    assert.object(options.pool, 'options.pool');
    assert.number(options.queryTimeout, 'options.queryTimeout');

    if (++CLIENT_ID >= 4294967295) // 2^32 -1
        CLIENT_ID = 1;

    this.client = options.client;
    this.client.on('error', this._handleClientError.bind(this));
    this.pool = options.pool; // the pool to which we belong.

    /*
     * "handle" is the connection handle we get back from the cueball pool.
     * When handle is non-null we're "checked out" of the pool, and when null
     * we're idle in-pool.
     */
    this.handle = null;

    this._moray_id = CLIENT_ID;
    this._defaultTimeout = options.queryTimeout;
    this._queryTimeout = options.queryTimeout;
    this._moray_had_err = null;
    this._moray_txn = false;
    this._reqid = null;

    this.log = options.log.child({
        component: 'PGClient',
        moray_id: this._moray_id
    }, true);
    EventEmitter.call(this);
}
util.inherits(PGClient, EventEmitter);

/*
 * The underlying Postgres will emit errors when it has a connection
 * problem. This can fire multiple times: once when the connection goes
 * away, and again if we try to make a query using this client. When
 * this happens, we mark this client as having failed so that the pool
 * will remove us once we're released.
 */
PGClient.prototype._handleClientError = function (err) {
    this.log.error({
        err: err,
        client: this
    }, 'pg: client emitted an error');

    this._moray_had_err = new VError(err, 'Postgres client failed');

    this.emit('conn-error', this._moray_had_err);
};


/*
 * Set a timeout for queries ("queryTimeout" in the configuration).
 */
PGClient.prototype.setTimeout = function setQueryTimeout(timeout) {
    assert.finite(timeout, 'timeout');
    assert.ok(timeout >= 0, 'timeout >= 0');
    this._queryTimeout = timeout;
};


/*
 * Set the request id for this client. This allows us to associate database
 * queries with a specific RPC being processed by Moray by passing it as an
 * argument to the 'query-*' dtrace probes.
 */
PGClient.prototype.setRequestId = function setRequestId(reqid) {
    assert.string(reqid, 'reqid');
    this._reqid = reqid;
};


/*
 * Called when the postgres client is released back into the pool. Currently,
 * all new RPCs will overwrite the request id anyway, but this guards against
 * accidential request id re-use by future users of this client.
 */
PGClient.prototype.clearRequestId = function clearRequestId() {
    assert.string(this._reqid, 'this._reqid');
    this._reqid = null;
};


/*
 * Restore default timeout in case it was changed, and return this
 * client back to the pool.
 */
PGClient.prototype.release = function clientRelease() {
    var ok = false;
    try {
        ok = pgAssert(this);
    } catch (_) {
        ok = false;
    }

    // We aren't intact, don't release ourselves back to the pool.
    if (!ok && this.handle) {
        var handle = this.handle;
        this.handle = null;
        handle.close();
        return;
    }

    this.setTimeout(this._defaultTimeout);
    this.clearRequestId();
    this.pool.release(this);
};


PGClient.prototype.query = function clientQuery(sql, args) {
    assert.string(sql, 'sql');
    assert.optionalArray(args, 'args');

    // Clean up whitespace so queries are normalized to DTrace
    sql = sql.replace(/(\r\n|\n|\r)/gm, '').replace(/\s+/, ' ');

    var log = this.log;
    var req;
    var res = new EventEmitter();
    var self = this;
    var timer;

    if (self._moray_had_err !== null) {
        /*
         * If we've hit a connection error, then we need to make sure to
         * avoid calling the node-postgres .query() method, since it won't
         * emit anything.
         */
        setImmediate(function () {
            res.emit('error', self._moray_had_err);
        });
        return res;
    }

    /*
     * Moray will periodically issue the query 'SELECT NOW() AS when' to
     * available PGClients as a health checking facility (see pgCheck).
     * These queries originate at Moray and are not associated with an RPC.
     * Therefore, the _reqid member will be null.
     */
    var reqid = (this._reqid === null) ? libuuid.create() : this._reqid;
    var ended = false;

    function done(event, arg) {
        if (ended) {
            return;
        }

        ended = true;

        self.removeListener('conn-error', onConnError);
        clearTimeout(timer);

        res.emit(event, arg);
    }

    function onConnError(err) {
        dtrace['query-conn-error'].fire(function () {
            return ([reqid, sql, err.toString()]);
        });

        setImmediate(done, 'error', err);
    }

    req = new pg.Query(sql, args);

    req.on('row', function onRow(row) {
        dtrace['query-row'].fire(function () {
            return ([reqid, sql, row]);
        });

        log.debug({
            req_id: reqid,
            client: self,
            row: row
        }, 'query: row');

        if (ended) {
            return;
        }

        clearTimeout(timer);
        res.emit('row', row);
    });

    req.on('end', function onQueryEnd(arg) {
        dtrace['query-done'].fire(function () {
            return ([reqid, sql, arg]);
        });

        log.debug({
            req_id: reqid,
            client: self,
            res: arg
        }, 'query: done');

        done('end', arg);
    });

    self.on('conn-error', onConnError);

    req.on('error', function onQueryError(err) {
        dtrace['query-error'].fire(function () {
            return ([reqid, sql, err.toString()]);
        });

        log.debug({
            req_id: reqid,
            client: self,
            err: err
        }, 'query: failed');

        /*
         * node-postgres's client.query() will fire "error" synchronously when
         * the client's connection dies (or is already dead), sometimes
         * resulting in this handler firing in the same tick as the
         * client.query() call. Since the PGClient.query() caller won't have
         * had an opportunity to set up their own "error" listener, we delay
         * firing the event until the next tick.
         */
        setImmediate(done, 'error', err);
    });

    if (this._queryTimeout > 0) {
        timer = setTimeout(function onRowTimeout() {
            var err = new QueryTimeoutError(sql);

            /*
             * Don't ROLLBACK, since it may or may not make it to the server,
             * and the query we just timed out may have placed things in a
             * weird state; just force a connection close to happen once the
             * connection is out of the pool.
             */
            self._moray_had_err = err;

            dtrace['query-timeout'].fire(function () {
                return ([reqid, sql]);
            });

            /*
             * We're timing out the query inside Moray, but the Postgres query
             * is still running. It may still return rows, return a SQL error,
             * or end due to connection problems. We won't emit anything
             * further on "res" after this point, but we will continue logging
             * and firing DTrace probes for anyone who's observing the process,
             * though.
             */
            done('error', err);
        }, this._queryTimeout);
    }

    this.client.query(req);

    dtrace['query-start'].fire(function () {
        return ([reqid, sql]);
    });

    log.debug({
        req_id: reqid,
        client: self,
        sql: sql,
        args: args
    }, 'pg.query: started');

    return res;
};


PGClient.prototype.begin = function transactionBegin(level, cb) {
    var self = this;

    if (typeof (level) === 'function') {
        cb = level;
        level = 'READ COMMITTED';
    }
    assert.func(cb, 'callback');

    var q = self.query('BEGIN TRANSACTION ISOLATION LEVEL ' + level);

    q.once('error', function onBeginTransactionError(err) {
        self._moray_had_err = new VError(err, 'Failed to begin transaction');
        cb(err);
    });

    q.once('end', function (_) {
        self._moray_txn = true;
        cb();
    });
};


PGClient.prototype.commit = function transactionCommit(cb) {
    assert.func(cb, 'callback');

    var self = this;

    function _cb(err) {
        self._moray_txn = false;
        self.release();
        cb(err);
    }

    /*
     * Don't time out the final "COMMIT" query. Sending back a
     * QueryTimeoutError would needlessly complicate things for
     * the consumer and for us, since Postgres would still be
     * processing the query after we give up on it, and possibly
     * succeed or fail. Handling this would make proper management
     * of our PG connections more complicated.
     *
     * Given that "timeout" doesn't actually introduce an
     * upper-bound on the overall Moray RPC but instead controls
     * how long we wait for a response to each of the SQL queries
     * we make, clients who actually want an upper-bound on how
     * long an RPC takes should implement the appropriate logic
     * on their end.
     */
    self.setTimeout(0);

    var q = self.query('COMMIT');

    q.once('error', function onCommitError(err) {
        self._moray_had_err = new VError(err, 'Failed to commit transaction');
        _cb(err);
    });

    q.once('end', function (_) {
        _cb(null);
    });
};


PGClient.prototype.rollback = function transactionRollback(cb) {
    assert.optionalFunc(cb, 'callback');

    var self = this;

    function _cb(err) {
        self._moray_txn = false;
        self.release();
        if (cb) {
            cb(err);
        }
    }

    /*
     * If "_moray_had_err" has been set by the point we're running the
     * .rollback() method, it's because one of the following has happened:
     *
     *   - We had a connection error
     *   - We failed to run BEGIN TRANSACTION (in which case _moray_txn is
     *     set to false)
     *   - We've timed out a query, and returned a QueryTimeoutError
     *
     * We could try to optimistically send ROLLBACK for the last case, but
     * given that we timed out the query, it's possible that there are either
     * connection issues, or we're waiting on a long-running query. We'll
     * continue to occupy a slot in the pool that we intend to abandon as soon
     * as the ROLLBACK returns, so failing fast here and letting Postgres abort
     * the transaction itself seems to be a better approach.
     */
    if (!self._moray_txn || self._moray_had_err !== null) {
        _cb();
        return;
    }

    var q = self.query('ROLLBACK');

    q.once('error', function onRollbackError(err) {
        self._moray_had_err = new VError(err, 'Failed to rollback transaction');
        _cb(err);
    });

    q.once('end', function (_) {
        _cb(null);
    });
};


PGClient.prototype.destroy = function closePGClient() {
    var self = this;

    self.log.warn({ client: self }, 'pg: destroying connection');

    self.client.end(function () {
        self.client = null;
        self._deadbeef = true;
    });
    self.emit('close');
};


// --- End Postgres Client Wrapper Class



// --- Pool Functions

function pgAssert(_pg) {
    assert.ok(_pg, 'pg client wrapper');
    assert.ok(_pg.client, 'pg handle');
    assert.ok(_pg.client.connection, 'pg connection');
    assert.ok(_pg.client.connection.stream, 'pg stream');
    assert.ok(_pg.client.connection.stream.readable, 'pg readable');
    assert.ok(!_pg.client.connection.stream.destroyed, 'pg not destroyed');
    assert.ok(_pg.client.connection.stream.writable, 'pg writable');
    assert.equal(false, _pg._moray_txn, 'finished transaction');

    return (_pg._moray_had_err === null);
}

/*
 * node-cueball's checker interface expects does an "implied claim" on the idle
 * connection we're checking: it expects us to call either handle.release() for
 * check-success, or handle.close() for check-failure.
 */
function pgCheck(handle, pgc) {
    var ok = false;
    var req = pgc.query('SELECT NOW() AS when');
    req.once('error', function () {
        handle.close();
    });
    req.once('row', function (_) {
        ok = true;
    });
    req.once('end', function () {
        if (ok) {
            handle.release();
        } else {
            handle.close();
        }
    });
}

function pgCreate(opts) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.pool, 'options.pool');
    assert.optionalString(opts.url, 'options.url');
    assert.optionalString(opts.user, 'options.user');

    var log = opts.log;

    function _pgCreate(backend) {
        var client = null;
        var copts = null;
        var timer = null;
        var pgc = null;

        if (opts.url) {
            copts = {
                connectionString: opts.url,
                keepAlive: true
            };
        } else {
            copts = {
                user: opts.user,
                host: backend.address,
                port: backend.port,
                database: DBNAME,
                keepAlive: true
            };
        }

        client = new pg.Client(copts);

        function onError(err) {
            log.fatal({
                options: copts,
                err: err
            }, 'unable to create PG client');

            clearTimeout(timer);
            client.removeAllListeners('connect');

            if (client.connection && client.connection.stream) {
                client.connection.stream.destroy();
            }

            // We have not successfully created a client.
            pgc._handleClientError(err);
        }

        client.once('error', onError);

        pgc = new PGClient({
            client: client,
            connectTimeout: opts.connectTimeout,
            log: opts.log,
            pool: opts.pool,
            queryTimeout: opts.queryTimeout
        });

        if (opts.connectTimeout > 0) {
            timer = setTimeout(function () {
                /*
                 * We don't remove the "error" listener since node may
                 * then throw for this timed out connection.
                 */
                client.removeAllListeners('connect');
                if (client.connection &&
                    client.connection.stream) {
                    client.connection.stream.destroy();
                }

                var t = opts.connectTimeout;
                pgc._handleClientError(new ConnectTimeoutError(t));
            }, opts.connectTimeout);
        }

        client.once('connect', function onConnect() {
            // Remove the timeout handlers
            clearTimeout(timer);
            client.removeListener('error', onError);
            pgc.emit('connect');
        });

        client.connect();
        return (pgc);
    }

    return (_pgCreate);
}

// --- End Pool Functions



// --- API

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
    assert.object(options.resolver, 'options.resolver');
    assert.number(options.maxConnections, 'options.maxConnections');
    assert.number(options.minSpareConnections, 'options.minSpareConnections');
    assert.number(options.targetClaimDelay, 'options.targetClaimDelay');
    assert.number(options.queryTimeout, 'options.queryTimeout');
    assert.number(options.maxQueueLength, 'options.maxQueueLength');
    assert.object(options.collector, 'options.collector');
    assert.string(options.domain, 'options.domain');

    this.checkInterval = options.checkInterval;
    this.connectTimeout = options.connectTimeout;
    this.log = options.log.child({
        component: 'pgpool',
        serializers: SERIALIZERS
    });
    this.maxConnections = options.maxConnections;
    this.queryTimeout = options.queryTimeout;
    this.maxQueueLength = options.maxQueueLength;
    this.collector = options.collector;

    /*
     * Create various metric collectors for measuring connections and
     * queue depth.
     */
    this.openGauge = this.collector.gauge({
        name: 'postgres_connections_open',
        help: 'count of open postgres connections'
    });
    this.pendingGauge = this.collector.gauge({
        name: 'postgres_connections_pending',
        help: 'count of pending postgres connections'
    });
    this.availableGauge = this.collector.gauge({
        name: 'postgres_connections_available',
        help: 'count of available postgres connections'
    });
    this.queueDepthGauge = this.collector.gauge({
        name: 'request_queue_depth',
        help: 'number of queued requests'
    });

    var pgOpts = {
        connectTimeout: this.connectTimeout,
        firstResultTimeout: this.firstResultTimeout,
        log: this.log,
        pool: this,
        queryTimeout: this.queryTimeout,
        user: options.user,
        url: options.url
    };
    /*
     * Our node-cueball connection-pool constructor is built with the smarts
     * required to connect to the current primary (see pgOpts above).
     *
     * Note: our custom resolver takes care of connecting to the current
     * primary -- so some options provided to node-cueball are dummy values
     * (i.e. not used to actually do connections).
     */
    this.pool = new mod_cueball.ConnectionPool({
        domain: options.domain,
        service: '_postgres._tcp',
        defaultPort: 12345, // dummy value not used.
        spares: options.minSpareConnections,
        maximum: options.maxConnections,
        constructor: pgCreate(pgOpts),
        resolver: options.resolver,
        checkTimeout: options.checkInterval,
        checker: pgCheck,
        targetClaimDelay: options.targetClaimDelay,
        recovery: {
            default: {
                timeout: this.connectTimeout,
                maxTimeout: 10000,
                retries: 10,
                delay: 250,
                maxDelay: 2500
            }
        },
        collector: options.collector
    });

    this.url = options.url;

    EventEmitter.call(this);
}
util.inherits(PGPool, EventEmitter);


PGPool.prototype.close = function close(cb) {
    var self = this;

    this.log.trace({pool: self.pool}, 'close: entered');
    this.pool.stop();

    self._deadbeef = true;

    self.log.trace({pool: self.pool}, 'close: closed');
    if (typeof (cb) === 'function')
        cb();

    self.emit('close');
};


PGPool.prototype.pg = function checkout(callback) {
    assert.func(callback, 'callback');

    var log = this.log;
    var stats = this.pool.getStats();

    log.trace({pool: this.pool}, 'checkout: entered');

    if (this.maxQueueLength > 0) {
        if (stats.waiterCount >= this.maxQueueLength) {
            setImmediate(callback, createOverloadedError());
            return;
        }
    }

    dtrace['pool-checkout'].fire(function onClaimEnabled() {
        return [
            stats.totalConnections,
            stats.idleConnections,
            stats.pendingConnections,
            stats.waiterCount
        ];
    });

    this.pool.claim(function afterClaim(err, h, client) {
        if (err) {
            if (VError.hasCauseWithName(err, 'PoolFailedError')) {
                callback(createNoBackendError());
                return;
            }

            if (VError.hasCauseWithName(err, 'ClaimTimeoutError')) {
                if (stats.idleConnections === 0 &&
                    (stats.totalConnections - stats.pendingConnections) <= 2) {
                    /*
                     * We need to disambiguate the case where we have timed out
                     * because we can't connect to the database, so that we can
                     * return an appropriate error indicating whether we're just
                     * overloaded, or service is completely unavailable.
                     *
                     * When all of our connection slots are trying to connect,
                     * then we assume that the database is currently down. The
                     * possible difference of 2 is to handle the point where a
                     * connection slot has failed, and still counts towards
                     * total connections but not pending connections.
                     */
                    callback(createNoBackendError());
                } else {
                    callback(createOverloadedError());
                }
                return;
            }

            log.trace(err, 'checkout: failed');
            callback(err);
        } else {
            /*
             * Save the pool-handle into our client object (callback
             * will need it for pool management).
             */
            client.handle = h;
            log.trace({
                client: client
            }, 'checkout: done');

            callback(null, client);
        }
    });
};


PGPool.prototype.release = function release(client) {
    assert.object(client, 'client');

    if (client.handle) {
        var handle = client.handle;
        client.handle = null;
        handle.release();
    }

    this.log.trace({
        client: client,
        pool: this.pool
    }, 'release: done');
};

PGPool.prototype.toString = function toString() {
    var str = '[object PGPool <' +
        'checkInterval=' + this.checkInterval + ', ' +
        'maxConnections=' + this.maxConnections + ', ' +
        'url=' + this.url + '>]';

    return (str);
};

/*
 * Record the connection counts and request queue length.
 */
PGPool.prototype.getPoolStats = function getPoolStats() {
    var stats = this.pool.getStats();

    this.openGauge.set(stats.totalConnections);
    this.pendingGauge.set(stats.pendingConnections);
    this.availableGauge.set(stats.idleConnections);
    this.queueDepthGauge.set(stats.waiterCount);
};

function pgError(e) {
    var err;
    var msg;

    switch (e.code) {
    case '23505':
        /* JSSTYLED */
        msg = /.*violates unique constraint.*/.test(e.message);
        if (msg) {
            // Key (str_u)=(hi) already exists
            /* JSSTYLED */
            msg = /.*\((.*)\)=\((.*)\)/.exec(e.detail);
            err = new UniqueAttributeError(err, msg[1], msg[2]);
        } else {
            err = e;
        }
        break;
    case '42601':
    case '42701':
        err = new InternalError(e, 'Invalid SQL: %s', e.message);
        break;
    default:
        err = e;
        break;
    }

    return (err);
}


function typeToPg(type) {
    assert.string(type, 'type');

    if (TYPES.hasOwnProperty(type)) {
        return TYPES[type].pg;
    } else {
        throw new InvalidIndexDefinitionError(type);
    }
}

// --- Initialize

pg.types.setTypeParser(TSTZRANGE_OID, fixupDateRange);
pg.types.setTypeParser(TSTZRANGE_ARRAY_OID, function (val) {
    if (val === null) {
        return null;
    }

    var p = pg.types.arrayParser.create(val);
    return p.parse().map(fixupDateRange);
});

// --- Exports

module.exports = {

    createPool: function createPool(opts) {
        assert.object(opts, 'opts');

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
        number('maxConnections', 5);
        number('minSpareConnections', 2);
        number('queryTimeout', 0);
        number('maxQueueLength', 2000);
        number('targetClaimDelay', 500);

        return (new PGPool(opts));
    },

    PGPool: PGPool,

    pgError: pgError,

    typeToPg: typeToPg,

    ConnectTimeoutError: ConnectTimeoutError,

    QueryTimeoutError: QueryTimeoutError
};
