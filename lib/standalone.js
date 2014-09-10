// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var bunyan = require('bunyan');
var once = require('once');
var VError = require('verror').VError;

var postgresPool = require('./pg');



///--- Globals

var sprintf = util.format;

var SERIALIZERS = {
    pg: function _serializePg(c) {
        return (c ? c._moray_id : undefined);
    },
    pool: function (pool) {
        var obj = {};

        Object.keys(pool).forEach(function (k) {
            obj[k] = pool[k] ? pool[k].url : undefined;
        });

        return (obj);
    },
    db: function (db) {
        var p = db.pool;

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
//
// Simple Wrappers/Helpers First
//


function createPGPool(opts) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.pgConfig, 'options.pgConfig');
    assert.string(opts.url, 'options.url');

    var pool = postgresPool.createPool({
        log: opts.log,
        checkInterval: opts.pgConfig.checkInterval,
        connectTimeout: opts.pgConfig.connectTimeout,
        maxConnections: opts.pgConfig.maxConnections,
        maxIdleTime: opts.pgConfig.maxIdleTime,
        role: opts.role,
        queryTimeout: opts.pgConfig.queryTimeout,
        url: opts.url
    });

    return (pool);
}



////--- API

function Standalone(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.pg, 'options.pg');
    assert.string(options.url, 'options.url');

    var self = this;
    EventEmitter.call(this);

    this.log = options.log.child({
        component: 'standalone',
        serializers: SERIALIZERS
    });
    this.pgConfig = options.pg;
    this.timeout = parseInt((options.timeout || 60000), 10);
    this._whatami = 'Standalone';
    this.pgUser = options.pg.user;
    this.url = options.url;

    setImmediate(function () {
        self._init();
    });
}
util.inherits(Standalone, EventEmitter);


Standalone.prototype.pg = function pg(options, callback) {
    if (typeof (options) === 'function') {
        callback = options;
        options = {};
    }
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    // XXX MANTA-734 - take out db selection logic for now and solely
    // target the primary
    var db = (this.database || {}).primary;
    var log = this.log;

    if (!db) {
        callback(new NoDatabasePeersError());
        return;
    }

    log.debug({
        db: db,
        options: options
    }, 'pg: entered');
    db.checkout(function pgCallback(err, client) {
        if (err) {
            log.debug(err, 'pg: failed');
            if (client)
                db.release(client);
            callback(err);
            return;
        }

        log.debug({pg: client}, 'pg: done');
        callback(null, client);
    });
};


Standalone.prototype.start = function start(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var log = this.log;

    this.pg(opts, function (pool_err, client) {
        if (pool_err) {
            cb(pool_err);
            return;
        }

        client.begin(function (err) {
            if (err) {
                log.debug({
                    pg: client,
                    err: err
                }, 'start: BEGIN failed');
                cb(err);
                return;
            }

            cb(null, client);
        });
    });
};


Standalone.prototype.toString = function toString() {
    var str = '[object Standalone <';
    str += 'listening=' + (this.listener ? 'yes' : 'no') + ', ';
    str += sprintf('url=%s', this.url);
    str += sprintf('timeout=%d', this.timeout);
    str += '>]';

    return (str);
};


Standalone.prototype.close = function close() {
    if (this.database && this.database.primary) {
        this.database.primary.close();
    }
    this.emit('close');
};



///--- Private Methods

Standalone.prototype._init = function _init() {
    var self = this;
    var log = this.log;

    log.debug('init: entered');
    self._refresh();
    setImmediate(self.emit.bind(self, 'ready'));
};


Standalone.prototype._refresh = function _refresh() {
    var self = this;

    // If there is an existing connection pool, close it:
    if (self.database && self.database.primary) {
        self.database.primary.close();
    }

    // Mock up a topology based on our single PostgreSQL URL:
    self.database = {
        primary: createPGPool({
            log: self.log,
            pgConfig: self.pgConfig,
            role: 'primary',
            url: self.url
        })
    };
};



///--- Exports

module.exports = {
    createClient: function createClient(options) {
        return (new Standalone(options));
    }
};
