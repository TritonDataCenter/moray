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
var manatee = require('node-manatee');
var url = require('url');

var postgresPool = require('./pg');

var NoDatabasePeersError = require('./errors').NoDatabasePeersError;


// --- Globals

var sprintf = util.format;

var DBNAME = process.env.MORAY_DB_NAME || 'moray';
var SERIALIZERS = {
    pg: function (c) {
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
        try {
            var s = db.getStats();

            return ({
                available: s.idleConnections,
                max: db.pool.maxConnections,
                size: s.totalConnections,
                waiting: s.waiterCount
            });
        } catch (_) {
            return null;
        }
    },
    err: bunyan.stdSerializers.err
};



// --- API

function Manatee(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.pg, 'options.pg');
    assert.string(options.pg.user, 'options.pg.user');
    assert.object(options.manatee, 'options.manatee');
    assert.object(options.collector, 'options.collector');

    var self = this;
    EventEmitter.call(this);

    this._log = options.log.child({
        component: 'manatee',
        serializers: SERIALIZERS
    });
    this._pgConfig = options.pg;
    this._whatami = 'Manatee';
    this._manateeCfg = options.manatee;
    self._manateeCfg.log = self._log;
    this._manatee = null;
    this._database = null;
    this._pgUser = options.pg.user;
    this._collector = options.collector;

    /*
     * Collect connection pool information when metrics are scraped.
     */
    self._collector.addTriggerFunction(function (_, cb) {
        if (self._database) {
            self._database.getPoolStats();
        }
        setImmediate(cb);
    });
    this._resolver = null;

    self._manatee = manatee.createClient(self._manateeCfg);
    self._manatee.on('ready', function () {
        self._log.info('manatee ready');
        self.emit('ready');
    });

    self._manatee.on('topology', function (urls) {
        self._log.info({urls: urls}, 'topology changed');
        self._refresh(urls);
    });
}
util.inherits(Manatee, EventEmitter);



// --- Exports

module.exports = {
    createClient: function createClient(options) {
        return (new Manatee(options));
    }
};



// --- API

Manatee.prototype.pg = function pg(callback) {
    assert.func(callback, 'callback');

    var self = this;
    var db = self._database;
    var log = self._log;

    if (!db) {
        setImmediate(callback, new NoDatabasePeersError(
            'no manatee peers available'));
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


Manatee.prototype.toString = function toString() {
    var self = this;
    var str = '[object Manatee <';
    str += sprintf('manatee=%j, ', self._manateeCfg);
    str += '>]';

    return (str);
};


Manatee.prototype.close = function close() {
    if (this._database) {
        this._database.close();
        this._database = null;
    }
    if (this._manatee) {
        this._manatee.close();
        this._manatee = null;
    }
    if (this._resolver) {
        this._resolver.stop();
        this._resolver = null;
    }
    this.emit('close');
};



// --- Private Methods


/**
 * This is our workhorse for managing Postgres connections. Whenever manatee
 * sends a notification we need to actually see if topology changed
 * (MANTA-1085), and if so shutdown the old connections and reconnect to the
 * new leader.
 *
 * Topology change means that one of:
 *
 * - a new peer was added
 * - a peer went away
 * - a peer changed roles
 *
 * However, moray only cares if the primary went away. For reference, the
 * topology object manatee emits is an array ordered by priority of each node
 * in the shard. i.e. [tcp://postgres@foo:5432, tcp://postgres@bar:5432,
 * tcp://postgres@baz:5432], then foo is the primary, bar is the
 * sync, and baz is the async
 *
 * Even though sync and async might change, we only care if the primary
 * changes.  The self._database object will change accordingly, FIF there's a
 * change in the primary.
 */
Manatee.prototype._refresh = function _refresh(topology) {
    var self = this;
    var log = self._log;

    function createPGPool(opts) {
        assert.object(opts, 'options');
        assert.object(opts.log, 'options.log');
        assert.object(opts.pgConfig, 'options.pgConfig');
        assert.string(opts.url, 'options.url');
        assert.object(opts.resolver, 'options.resolver');

        log.info({url: opts.url}, '_refresh.createPGPool: create new pg pool');

        var pool = postgresPool.createPool({
            log: opts.log,
            checkInterval: opts.pgConfig.checkInterval,
            connectTimeout: opts.pgConfig.connectTimeout,
            maxConnections: opts.pgConfig.maxConnections,
            maxIdleTime: opts.pgConfig.maxIdleTime,
            maxQueueLength: opts.pgConfig.maxQueueLength,
            role: 'primary',
            queryTimeout: opts.pgConfig.queryTimeout,
            url: opts.url,
            resolver: opts.resolver,
            collector: opts.collector
        });

        return (pool);
    }

    var previous = self._database;
    var newPrimary = topology[0];
    // update the url's user, and table name to what moray uses.
    if (newPrimary) {
        newPrimary = url.parse(newPrimary);
        newPrimary.auth = self._pgUser;
        newPrimary.pathname = DBNAME;
        newPrimary = url.format(newPrimary);
    }

    log.warn({
        currentPrimary: previous ? previous.url : 'none',
        newPrimary: newPrimary ? newPrimary : 'none'
    }, '_refresh: manatee topology updated');

    /*
     * We update the backend in our node-cueball pool resolver, and
     * delegate pool-maintenance to the node-cueball internals.
     *
     * Before we used node-cueball, we used to re-create the pool from scratch
     * for *any* config change -- even if the primary stayed the same, because
     * the pool's primary connections might not be intact.
     */
    if (newPrimary) {
        log.info({
            newPrimary: newPrimary
        }, '_refresh: connecting to new primary');

        if (self._database && self._resolver) {
            self._resolver.updateBackend(newPrimary);
        } else {
            /*
             * We're using node-cueball in a slightly weird way here:
             *
             * Manatee handles our failover (which is non-DNS driven). We only
             * ever want to have a single-backend in node-cueball's
             * resolver. And the resolver "backend" data consists of a
             * PG-Connection-Info string for use by the pooled clients.
             */
            self._resolver = new postgresPool.PGResolver();
            self._resolver.start();
            self._resolver.updateBackend(newPrimary);

            self._database = createPGPool({
                log: log,
                pgConfig: self._pgConfig,
                url: newPrimary,
                resolver: self._resolver,
                collector: self._collector
            });
        }
    }
};
