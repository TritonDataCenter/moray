// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var pg = require('pg').native;
var pooling = require('pooling');
var restify = require('restify');
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



///--- Internal Functions

function pgError(e) {
        assert.ok(e);

        var err = new restify.InternalError(e, 'TODO: parse messages');
        return (err);
}

function pgCheckCallback(options) {
        assert.object(options);

        function pgCheck(client, callback) {
                client.query('SELECT NOW() AS when', callback);
        }

        return (pgCheck);
}


function pgCreateCallback(options) {
        assert.object(options);

        function pgCreate(callback) {
                var client = new pg.Client(options.url);
                var log = options.log;

                client.connect(function (err) {
                        if (err) {
                                log.error(err, 'unable to create PG client');
                                return (callback(err));
                        }

                        client._moray_id = ++CLIENT_ID;
                        return (callback(null, client));
                });
        }

        return (pgCreate);
}


function pgDestroyCallback(options) {
        assert.object(options);

        function pgDestroy(client) {
                client.removeAllListeners('end');
                client.removeAllListeners('error');
                client.end();
        }

        return (pgDestroy);
}


function createPool(options) {
        assert.object(options);

        var pool = pooling.createPool({
                checkInterval: options.checkInterval,
                max: options.maxConnections,
                maxIdleTime: options.maxIdleTime,
                name: 'postgres',

                // Callbacks
                check: pgCheckCallback(options),
                create: pgCreateCallback(options),
                destroy: pgDestroyCallback(options)
        });

        return (pool);
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
        assert.object(options.log, 'options.log');
        assert.number(options.maxConnections, 'options.maxConnections');
        assert.number(options.maxIdleTime, 'options.maxIdleTime');
        assert.object(options.zk, 'options.zk');

        var self = this;

        EventEmitter.call(this);

        this.checkInterval = options.checkInterval;
        this.maxConnections = options.maxConnections;
        this.maxIdleTime = options.maxIdleTime;
        this.pool = createPool(options);
        this.log = options.log.child({
                clazz: 'PGClient',
                serializers: SERIALIZERS
        });
        this.url = options.url;


}
util.inherits(PGClient, EventEmitter);


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
                        },'checkout: done');
                        callback(null, client);
                }
        });
};


PGClient.prototype.commit = function commit(client, callback) {
        assert.object(client, 'client');
        assert.func(callback, 'callback');

        var log = this.log;
        var opts = {
                client: client,
                sql: 'COMMIT'
        };
        var self = this;
        log.trace({client: client}, 'commit: entered');

        this.query(opts, function (err) {
                if (err) {
                        log.trace({
                                client: client,
                                err: err
                        }, 'commit: failed');

                        self.rollback(client, function _forceRollbackCb() {
                                callback(pgError(err));
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
        assert.func(callback, 'callback');

        var log = this.log;
        var opts = {
                client: client,
                sql: 'ROLLBACK'
        };
        var self = this;
        log.trace({client: client}, 'rollback: entered');

        this.query(opts, function (err) {
                log.trace({
                        client: client,
                        err: err
                }, 'rollback: done');
                self.release(client);
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

        this.checkout(function (err, client) {
                if (err) {
                        log.trace(err, 'start: checkout failed');
                        callback(err);
                } else {
                        log.trace({client: client}, 'start: issuing BEGIN');
                        var opts = {
                                client: client,
                                sql: 'BEGIN'
                        }
                        self.query(opts, function (err) {
                                if (err) {
                                        log.trace({
                                                client: client,
                                                err: err
                                        }, 'start: BEGIN failed');
                                        self.release(client);
                                        callback(pgError(err));
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
//Postgres.prototype.query = function query(client, sql, values, callback) {
PGClient.prototype.query = function query(options, callback) {
        assert.object(options, 'options');
        assert.object(options.client, 'options.client');
        assert.string(options.sql, 'options.sql');
        if (options.values)
                assert.ok(Array.isArray(options.values));
        assert.func(callback, 'callback');

        var client = options.client;
        var log = this.log;
        var qid = uuid.v1().substr(0, 7);
        var req;
        var rows = [];

        log.trace({
                client: client,
                query_id: qid,
                sql: options.sql,
                values: options.values
        }, 'query: entered');

        req = client.query(options.sql, options.values);

        req.on('row', function (row) {
                log.trace({
                        client: client,
                        query_id: qid,
                        row: row,
                }, 'query: row received');

                if (callback)
                        rows.push(row);
        });

        req.once('error', function (err) {
                req.removeAllListeners('end');
                req.removeAllListeners('row');

                log.trace({
                        client: client,
                        err: err,
                        query_id: qid
                }, 'query: error');

                if (callback)
                        callback(pgError(err));
        });

        req.once('end', function () {
                req.removeAllListeners('error');
                req.removeAllListeners('row');

                log.trace({
                        client: client,
                        query_id: qid
                }, 'query: complete');

                if (callback)
                        callback(null, rows);
        });

        return (req);
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


        // var err;
        // var msg;

        // switch (e.code) {
        // case '22P02':
        //         err = new InvalidArgumentError(e.message);
        //         break;
        // case '23505':
        //         /* JSSTYLED */
        //         msg = /.*violates unique constraint.*/.test(e.message);
        //         if (msg) {
        //                 // This PG errror looks like:
        //                 // Key (email)=(foo@joyent.com) already exists
        //                 /* JSSTYLED */
        //                 msg = /\((.*)\)=\((.*)\)/.exec(e.detail);
        //                 err = new UniqueAttributeError(msg[1], msg[2]);
        //         } else {
        //                 err = new InvalidArgumentError(e.detail);
        //         }
        //         break;
        // case '42601':
        // case '42701':
        //         err = new InternalError('Invalid (moray) SQL: %s', e.message);
        //         break;
        // case '42P01':
        //         /* JSSTYLED */
        //         msg = /.*relation "(\w+)_entry".*/.exec(e.message);
        //         err = new BucketNotFoundError(msg ? msg[1] : '');
        //         break;
        // case '42P07':
        //         /* JSSTYLED */
        //         msg = /.*relation "(\w+)_entry.*"/.exec(e.message);
        //         err = new BucketAlreadyExistsError(msg ? msg[1] : '');
        //         break;
        // default:
        //         console.error('pgError: untranslated exception: %s', util.inspect(e));
        //         err = new InternalError(e.message);
        //         break;
        // }
        // err.cause = e.stack;
        // return err;

