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

var CLIENT_ID = 1;

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
        assert.object(options.log, 'options.log');
        assert.number(options.maxConnections, 'options.maxConnections');
        assert.number(options.maxIdleTime, 'options.maxIdleTime');

        var self = this;

        EventEmitter.call(this);

        this.checkInterval = options.checkInterval;
        this.log = options.log.child({
                clazz: 'PGClient',
                serializers: SERIALIZERS
        });
        this.maxConnections = options.maxConnections;
        this.maxIdleTime = options.maxIdleTime;
        this.pool = pooling.createPool({
                checkInterval: options.checkInterval,
                max: options.maxConnections,
                maxIdleTime: options.maxIdleTime,
                name: 'postgres',

                check: function pgCheck(client, cb) {
                        client.query('SELECT NOW() AS when', cb);
                },
                create: function pgCreate(cb) {
                        var client = new pg.Client(options.url);
                        var log = options.log;

                        client.connect(function (err) {
                                if (err) {
                                        log.error({
                                                err: err
                                        }, 'unable to create PG client');
                                        return (cb(err));
                                }

                                client._moray_id = ++CLIENT_ID;
                                client.begin = function begin(callback) {
                                        client.query('BEGIN',
                                                     function (beginErr) {
                                                             callback(beginErr);
                                                     });
                                };
                                client.commit = function _commit(callback) {
                                        self.commit(client, callback);
                                };

                                var _q = client.query.bind(client);
                                client.query = function query(c, v, callback) {
                                        log.trace({
                                                sql: c
                                        }, 'pg.query: entered');
                                        return (_q.apply(client, arguments));
                                };

                                client.release = function _release() {
                                        self.pool.release(client);
                                };
                                client.rollback = function _rollback(callback) {
                                        self.rollback(client, callback);
                                };
                                return (cb(null, client));
                        });
                },
                destroy: function pgDestroy(client) {
                        client.removeAllListeners('end');
                        client.removeAllListeners('error');
                        client.removeAllListeners('row');
                        client.end();
                }
        });
        this.url = options.url;

        this.pool.on('death', this.emit.bind(this, 'death'));
        this.pool.on('drain', this.emit.bind(this, 'drain'));
        this.pool.once('end', this.emit.bind(this, 'end'));
}
util.inherits(PGClient, EventEmitter);


PGClient.prototype.close = function close(cb) {
        var self = this;
        this.pool.shutdown(function () {
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
                if (err) {
                        log.trace({
                                client: client,
                                err: err
                        }, 'commit: failed');

                        self.rollback(client, function _forceRollbackCb() {
                                callback(err);
                        });

                } else {
                        log.trace({
                                client: client
                        }, 'commit: done');

                        self.pool.release(client);
                        callback(null);
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


PGClient.prototype.rollback = function rollback(client, callback) {
        assert.object(client, 'client');
        if (callback)
                assert.func(callback, 'callback');

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
                        var opts = {
                                client: client,
                                sql: 'BEGIN'
                        };
                        self.query(opts, function (err) {
                                if (err) {
                                        log.trace({
                                                client: client,
                                                err: err
                                        }, 'start: BEGIN failed');
                                        self.release(client);
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
                'url=' + this.url + '>]';

        return (str);
};



///--- Exports

module.exports = {

        createClient: function createClient(options) {
                var opts = clone(options);
                opts.log = options.log;
                opts.checkInterval = opts.checkInterval || 60000;
                opts.maxConnections = opts.maxConnections || 5;
                opts.maxIdleTime = opts.maxIdleTime || 120000;
                return (new PGClient(opts));
        },

        Client: PGClient

};
