// Copyright (c) 2013 Joyent, Inc.  All rights reserved.
// vim: set ts=4 sts=4 sw=4 et:

var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var libuuid = require('libuuid');
var once = require('once');
var sqlite3 = require('sqlite3');
var pooling = require('pooling');

var dtrace = require('./dtrace');


///--- Globals

var slice = Array.prototype.slice;
var sprintf = util.format;

var CLIENT_ID = 0;

var SERIALIZERS = {
  /* XXX client serialiser */
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



///--- SQLite Query Wrapper

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
// SQLite connection
function query(sql, args) {
    assert.string(sql, 'sql');
    if (typeof (arguments[arguments.length - 1]) === 'function')
        throw new TypeError('query: callback style illegal');

    // XXX we should do more validation here, as the SQLite API
    // is somewhat different to the PostgreSQL API

    // Clean up whitespace so queries are normalized to DTrace
    sql = sql.replace(/(\r\n|\n|\r)/gm, '').replace(/\s+/, ' ');

    var done;
    var log = this.log;
    var res = new EventEmitter();
    var self = this;
    var timer;
    var reqid;

    var ignore_events = false;
    done = once(function endOrError(event, arg) {
        ignore_events = true;

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
        qCleanup(null, res);
    });

    var err_cb = function (err) {
        if (ignore_events)
            return;

        if (reqid === undefined)
            reqid = libuuid.create();
        dtrace['query-error'].fire(function () {
            return ([reqid, sql, err.toString()]);
        });

        done('error', err);
    };

    var row_cb = function (err, row) {
        if (ignore_events)
            return;

        if (err) {
            err_cb(err);
            return;
        }

        if (reqid === undefined)
            reqid = libuuid.create();
        dtrace['query-row'].fire(function () {
            return ([reqid, sql, row]);
        });
        log.debug({client: self, row: row}, 'query: row');

        clearTimeout(timer);
        res.emit('row', row);
    };

    var end_cb = function (err) {
        if (ignore_events)
            return;

        if (err) {
            err_cb(err);
            return;
        }

        if (reqid === undefined)
            reqid = libuuid.create();
        dtrace['query-done'].fire(function () {
            return ([reqid, sql, arg]);
        });

        done('end', arg);
    };

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
    }, 'sqlite.query: started');

    if (arguments.length === 1) {
        this.each(arguments[0], row_cb, end_cb);
    } else if (arguments.length === 2) {
        if (!Array.isArray(arguments[1]))
            throw new TypeError('query: parameter 2 must be array');

        this.each(arguments[0], arguments[1], row_cb, end_cb);
    } else {
        throw new TypeError('query: must provide 1 or 2 valid arguments');
    }

    return (res);
}

///--- End Query API



///--- Pool Functions

function sqliteSetup(options) {
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

    client.log = log.child({
        component: 'SQLiteClient',
        moray_id: client._moray_id
    }, true);
    client.release = pool.release.bind(pool, client);

    // Provide a PostgreSQL-like query() API on top of SQLite
    client.query = query.bind(client);

    // Some friendly wrappers
    client.begin = function begin(level, cb) {
        if (typeof (level) === 'function') {
            cb = level;
        }
        assert.func(cb, 'callback');
        cb = once(cb);

        // In SQLite, all transactions are SERIALIZABLE, the highest isolation
        // level, so we ignore whatever level was passed in by the caller
        var q = client.query('BEGIN TRANSACTION');
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


function sqliteAssert(_sqlite) {
    assert.ok(_sqlite, 'sqlite handle');

    return (!_sqlite._moray_had_err);
}


function sqliteCheck(options) {
    function _sqliteCheck(client, cb) {
        var ok = false;
        var req = client.query('SELECT 12345 AS sentinel');
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

    return (_sqliteCheck);
}


function sqliteCreate(opts) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.pool, 'options.pool');
    assert.string(opts.filename, 'options.filename');

    var log = opts.log;

    function _sqliteCreate(cb) {

        cb = once(cb);

        var client = new sqlite3.Database(opts.filename, sqlite3.OPEN_READWRITE);
        if (opts.connectTimeout > 0) {
            var timer = setTimeout(function () {
                // do not remove error listener as node may
                // still throw
                client.removeAllListeners('open');
                if (client.connection &&
                    client.connection.stream) {
                    client.connection.stream.destroy();
                }

                var t = opts.connectTimeout;
                cb(new ConnectTimeoutError(t));
            }, opts.connectTimeout);
        }

        client.once('open', function onOpen() {
            clearTimeout(timer);

            client.removeAllListeners('error');

            sqliteSetup({
                client: client,
                connectTimeout: opts.connectTimeout,
                log: opts.log,
                pool: opts.pool,
                queryTimeout: opts.queryTimeout
            });

            cb(null, client);
        });

        client.once('error', function onError(err) {
            log.fatal(err, 'unable to create SQLite client');

            clearTimeout(timer);
            client.removeAllListeners('open');

            // XXX handle close error?  is this close() even necessary?
            client.close();

            cb(err);
        });
    }

    return (_sqliteCreate);
}


function sqliteDestroy(opts) {
    var log = opts.log;

    function _sqliteDestroy(client) {
        log.warn({client: client}, 'sqlite: destroying connection');

        // XXX should we handle close() errors?
        client.close();

        client._deadbeef = true;
    }

    return (_sqliteDestroy);
}

///--- End Pool Functions



///--- API

/**
 * Creates a new (pooled) SQLite client.
 *
 * var db = new SQLitePool({
 *     filename: 'database_file',
 *     log: new Logger({
 *         stream: process.stderr,
 *         serializers: Logger.stdSerializers
 *     }),
 *     maxConns: 100,
 *     idleTimeout: 60000
 * });
 *
 * @constructor
 * @arg {object} options             - Standard options object
 * @arg {object} options.log         - (bunyan) Logger instance.</br/>
 * @arg {string} options.filename    - SQLite filename.
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
function SQLitePool(options) {
    assert.object(options, 'options');
    assert.number(options.checkInterval, 'options.checkInterval');
    assert.number(options.connectTimeout, 'options.connectTimeout');
    assert.object(options.log, 'options.log');
    assert.number(options.maxConnections, 'options.maxConnections');
    assert.number(options.maxIdleTime, 'options.maxIdleTime');
    assert.number(options.queryTimeout, 'options.queryTimeout');
    assert.string(options.filename, 'options.filename');

    var self = this;

    this._whatami = 'SQLitePool';

    EventEmitter.call(this);

    this.checkInterval = options.checkInterval;
    this.connectTimeout = options.connectTimeout;
    this.log = options.log.child({
        component: 'sqlitepool',
        serializers: SERIALIZERS
    });
    this.maxConnections = options.maxConnections;
    this.maxIdleTime = options.maxIdleTime;
    this.queryTimeout = options.queryTimeout;

    var sqliteOpts = {
        connectTimeout: self.connectTimeout,
        firstResultTimeout: self.firstResultTimeout,
        log: self.log,
        pool: self,
        queryTimeout: self.queryTimeout,
        filename: options.filename
    };
    this.pool = pooling.createPool({
        assert: sqliteAssert,
        checkInterval: options.checkInterval,
        max: options.maxConnections,
        maxIdleTime: options.maxIdleTime,
        log: self.log,
        name: 'sqlitepool' + (options.role ? '-' + options.role : ''),
        check: sqliteCheck(sqliteOpts),
        create: sqliteCreate(sqliteOpts),
        destroy: sqliteDestroy(sqliteOpts)
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
util.inherits(SQLitePool, EventEmitter);


SQLitePool.prototype.close = function close(cb) {
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


SQLitePool.prototype.checkout = function checkout(callback) {
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


SQLitePool.prototype.release = function release(client) {
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


SQLitePool.prototype.toString = function toString() {
    var str = '[object SQLitePool <' +
        'checkInterval=' + this.checkInterval + ', ' +
        'maxConnections=' + this.maxConnections + ', ' +
        'maxIdleTime=' + this.maxIdleTime + ', ' +
        'filename=' + this.filename + '>]';

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

        return (new SQLitePool(opts));
    },

    SQLitePool: SQLitePool,

    ConnectTimeoutError: ConnectTimeoutError,

    QueryTimeoutError: QueryTimeoutError
};
