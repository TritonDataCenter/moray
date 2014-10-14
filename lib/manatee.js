/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var bunyan = require('bunyan');
var manatee = require('node-manatee');
var once = require('once');
var url = require('url');
var vasync = require('vasync');
var VError = require('verror').VError;
var xtend = require('xtend');

var postgresPool = require('./pg');



///--- Globals

var slice = Array.prototype.slice;
var sprintf = util.format;

var DBNAME = process.env.MORAY_DB_NAME || 'moray';
var SERIALIZERS = {
    db: function (d) {
        return (d ? {
            name: d.name,
            url: d.url,
            checkInterval: d.checkInterval,
            connectTimeout: d.connectTimeout,
            maxConnections: d.maxConnections,
            maxIdleTime: d.maxIdleTime,
            options: d.options,
            queryTimeout: d.queryTimeout,
            available: d.pool ? d.pool.available.length : 0,
            connections: d.pool ? d.pool.resources.length : 0
        } : undefined);
    },
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
            var p = db.pool;

            return ({
                available: p.available.length,
                max: p.max,
                size: p.resources.length,
                waiting: p.queue.length
            });
        } catch (e) {
            return null;
        }
    },
    err: bunyan.stdSerializers.err
};



////--- API

function Manatee(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.pg, 'options.pg');
    assert.string(options.pg.user, 'options.pg.user');
    assert.object(options.manatee, 'options.manatee');

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



///--- exports

module.exports = {
    createClient: function createClient(options) {
        return (new Manatee(options));
    }
};



///--- API

Manatee.prototype.pg = function pg(options, callback) {
    var self = this;
    if (typeof (options) === 'function') {
        callback = options;
        options = {};
    }
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    var db = self._database;
    var log = self._log;

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


Manatee.prototype.start = function start(opts, cb) {
    var self = this;
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var log = self._log;

    self.pg(opts, function (pool_err, client) {
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


Manatee.prototype.toString = function toString() {
    var self = this;
    var str = '[object Manatee <';
    str += sprintf('manatee=%j, ', self._manateeCfg);
    str += '>]';

    return (str);
};


Manatee.prototype.close = function close(cb) {
    if (this._database) {
        this._database.close();
        this._database = null;
    }
    if (this._manatee) {
        this._manatee.close();
        this._manatee = null;
    }
    this.emit('close');
};



///--- Private Methods


/**
 * This is our workhorse for managing Postgres connections Whenever maanatee
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

        log.info({url: opts.url}, '_refresh.createPGPool: create new pg pool');

        var pool = postgresPool.createPool({
            log: opts.log,
            checkInterval: opts.pgConfig.checkInterval,
            connectTimeout: opts.pgConfig.connectTimeout,
            maxConnections: opts.pgConfig.maxConnections,
            maxIdleTime: opts.pgConfig.maxIdleTime,
            role: 'primary',
            queryTimeout: opts.pgConfig.queryTimeout,
            url: opts.url
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

    log.info({
        currentPrimary: previous ? previous.url : 'none',
        newPrimary: newPrimary ? newPrimary : 'none'
    }, '_refresh: manatee topology updated');

    /*
     * always close the previous if it exists, since by definition the
     * primary has changed
     */
    if (previous) {
        log.info({
            oldPrimary: previous.url,
            newPrimary: newPrimary ? newPrimary : 'none'
        }, '_refresh: primary gone, closing');
        previous.close();
    }

    /*
     * MORAY-225 We always connect to the new primary regardless of the
     * previous primary. Even if the old primary == new primary, we still will
     * want to connect, since the old primary's connections might be
     * disconnected.
     */
    if (newPrimary) {
        log.info({
            newPrimary: newPrimary
        }, '_refresh: connecting to new primary');
        self._database = createPGPool({
            log: log,
            pgConfig: self._pgConfig,
            url: newPrimary
        });
    }
};
