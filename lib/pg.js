/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var once = require('once');
var pg = require('pg');
require('pg-parse-float')(pg);
var pooling = require('pooling');
var VError = require('verror');

var dtrace = require('./dtrace');

var mod_errors = require('./errors');
var ConnectTimeoutError = mod_errors.ConnectTimeoutError;
var InternalError = mod_errors.InternalError;
var InvalidIndexDefinitionError = mod_errors.InvalidIndexDefinitionError;
var NoDatabasePeersError = mod_errors.NoDatabasePeersError;
var QueryTimeoutError = mod_errors.QueryTimeoutError;
var UniqueAttributeError = mod_errors.UniqueAttributeError;

var TYPES = require('./types').TYPES;

// --- Globals

var CLIENT_ID = 0;

var SERIALIZERS = {
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

    this.pool = options.pool;

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
}


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
    assert.equal(false, this._moray_txn, 'finished transaction');
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

    // Moray will periodically issue the query 'SELECT NOW() AS when' to
    // available PGClients via the node-pooling library health checking
    // facility (see pgCheck). These queries originate at Moray and are not
    // associated with an RPC. Therefore, the _reqid member will be null.
    var reqid = (this._reqid === null) ? libuuid.create() : this._reqid;
    var aborted = false;

    function done(event, arg) {
        if (aborted) {
            return;
        }

        res.emit(event, arg);
        clearTimeout(timer);
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

        if (aborted) {
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

    req.on('error', function onQueryError(err) {
        dtrace['query-error'].fire(function () {
            return ([reqid, sql, err.toString()]);
        });

        log.debug({
            req_id: reqid,
            client: self,
            err: err
        }, 'query: failed');

        done('error', err);
    });

    if (this._queryTimeout > 0) {
        timer = setTimeout(function onRowTimeout() {
            var err = new QueryTimeoutError(sql);

            /*
             * Don't ROLLBACK, since it may or may not make
             * it to the server, and the query we just timed
             * out may have placed things in a weird state;
             * just force a connection close to happen once
             * the connection is out of the pool.
             */
            self._moray_had_err = err;

            dtrace['query-timeout'].fire(function () {
                return ([reqid, sql]);
            });

            /*
             * We're timing out the query inside Moray, but
             * the Postgres query is still running. It may
             * still return rows, return a SQL error, or end due
             * to connection problems. We don't emit anything
             * after this point, since we've already emitted an
             * "error" and will have replied to the client. We
             * do continue logging and firing DTrace probes for
             * anyone who's observing the process, though.
             */
            aborted = true;

            res.emit('error', err);
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

    return (res);
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


PGClient.prototype.close = function closePGClient() {
    var self = this;

    self.log.warn({ client: self }, 'pg: destroying connection');

    self.client.end(function () {
        self.client = null;
        self._deadbeef = true;
    });
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

    return (_pg._moray_had_err === null);
}


function pgCheck(_) {
    function _pgCheck(client, cb) {
        var ok = false;
        var req = client.query('SELECT NOW() AS when');
        req.once('error', cb);
        req.once('row', function (__) {
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

        var client = new pg.Client({
            connectionString: opts.url,
            keepAlive: true
        });

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

            var pgc = new PGClient({
                client: client,
                connectTimeout: opts.connectTimeout,
                log: opts.log,
                pool: opts.pool,
                queryTimeout: opts.queryTimeout,
                url: opts.url
            });

            cb(null, pgc);
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


function pgDestroy(_) {
    function _pgDestroy(client) {
        client.close();
    }

    return (_pgDestroy);
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
    assert.number(options.maxConnections, 'options.maxConnections');
    assert.number(options.maxIdleTime, 'options.maxIdleTime');
    assert.number(options.queryTimeout, 'options.queryTimeout');
    assert.number(options.maxQueueLength, 'options.maxQueueLength');
    assert.object(options.collector, 'options.collector');

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
        name: 'moray-pgpool' + (options.role ? '-' + options.role : ''),
        check: pgCheck(pgOpts),
        create: pgCreate(pgOpts),
        destroy: pgDestroy(pgOpts)
    });

    this.url = options.url;

    function reEmit(event) {
        self.log.debug('pool event: %s', event);
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
    assert.array(this.pool.queue, 'this.pool.queue');

    var log = this.log;

    log.trace({pool: this.pool}, 'checkout: entered');

    if (this.pool.queue.length >= this.maxQueueLength) {
        setImmediate(callback, new NoDatabasePeersError(
            'unable to acquire backend connection due to ' +
            'service being overloaded', 'OverloadedError',
                'maximum moray queue length reached'));
        return;
    }

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


PGPool.prototype.release = function release(client) {
    assert.object(client, 'client');

    this.pool.release(client);

    this.log.trace({
        client: client,
        pool: this.pool
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

/*
 * Record the connection counts and request queue length.
 */
PGPool.prototype.getPoolStats = function getPoolStats() {
    var poolState = this.state();

    this.openGauge.set(poolState.resources);
    this.pendingGauge.set(poolState.pending);
    this.availableGauge.set(poolState.available);
    this.queueDepthGauge.set(poolState.queue);
};

/*
 * Retrieve the underlying pool's connection state information.
 */
PGPool.prototype.state = function state() {
    return this.pool._state();
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
        number('maxIdleTime', 120000);
        number('maxConnections', 5);
        number('queryTimeout', 0);
        number('maxQueueLength', 2000);

        return (new PGPool(opts));
    },

    PGPool: PGPool,

    pgError: pgError,

    typeToPg: typeToPg,

    ConnectTimeoutError: ConnectTimeoutError,

    QueryTimeoutError: QueryTimeoutError
};
