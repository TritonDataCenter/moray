/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');

var postgresPool = require('./pg');

var NoDatabasePeersError = require('./errors').NoDatabasePeersError;


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
    assert.object(opts.collector, 'options.collector');
    assert.string(opts.url, 'options.url');
    assert.object(opts.resolver, 'options.resolver');

    var pool = postgresPool.createPool({
        log: opts.log,
        checkInterval: opts.pgConfig.checkInterval,
        connectTimeout: opts.pgConfig.connectTimeout,
        maxConnections: opts.pgConfig.maxConnections,
        maxIdleTime: opts.pgConfig.maxIdleTime,
        maxQueueLength: opts.pgConfig.maxQueueLength,
        role: opts.role,
        queryTimeout: opts.pgConfig.queryTimeout,
        url: opts.url,
        resolver: opts.resolver,
        collector: opts.collector
    });

    return (pool);
}



////--- API

function Standalone(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.pg, 'options.pg');
    assert.string(options.url, 'options.url');
    assert.object(options.collector, 'options.collector');

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
    this.collector = options.collector;

    /*
     * Collect connection pool information when metrics are scraped.
     */
    this.collector.addTriggerFunction(function (_, cb) {
        if (self.database && self.database.primary) {
            self.database.primary.getPoolStats();
        }
        setImmediate(cb);
    });


    setImmediate(function () {
        self._init();
    });
}
util.inherits(Standalone, EventEmitter);


Standalone.prototype.pg = function pg(callback) {
    assert.func(callback, 'callback');

    // XXX MANTA-734 - take out db selection logic for now and solely
    // target the primary
    var db = (this.database || {}).primary;
    var log = this.log;

    if (!db) {
        setImmediate(callback, new NoDatabasePeersError(
            'standalone database not available'));
        return;
    }

    log.debug({
        db: db
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


Standalone.prototype.start = function start(cb) {
    var log = this.log;

    this.pg(function (pool_err, client) {
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
        if (self.resolver) {
            self.resolver.stop();
            self.resolver = null;
        }
        self.database.primary.close();
    }

    /*
     * Create a resolver instance for node-cueball to retrieve
     * postgres-connection strings.
     */
    self.resolver = new postgresPool.PGResolver();
    self.resolver.start();
    self.resolver.updateBackend(self.url);

    // Mock up a topology based on our single PostgreSQL URL:
    self.database = {
        primary: createPGPool({
            log: self.log,
            pgConfig: self.pgConfig,
            role: 'primary',
            url: self.url,
            resolver: self.resolver,
            collector: self.collector
        })
    };
};



///--- Exports

module.exports = {
    createClient: function createClient(options) {
        return (new Standalone(options));
    }
};
