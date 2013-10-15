// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var bunyan = require('bunyan');
var once = require('once');
var vasync = require('vasync');
var VError = require('verror').VError;
var xtend = require('xtend');
var zkplus = require('zkplus');

var postgresPool = require('./pg');



///--- Globals

var slice = Array.prototype.slice;
var sprintf = util.format;

var DBNAME = process.env.MORAY_DB_NAME || 'moray';
var PG_FMT = 'tcp://postgres@%s:5432/%s';
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


//
// manatee writes something like: 10.99.99.14-123456
// sort by the number after the -, and you'll have:
// [primary, sync, async]
// This method turns those children into a list of URLs
//
function childrenToURLs(children) {
    function compare(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }

    var urls = (children || []).sort(compare).map(function (c) {
        var ip = c.split('-')[0];
        return (sprintf(PG_FMT, ip, DBNAME));
    });

    return (urls);
}


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


function createZkClient(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.arrayOfObject(opts.servers, 'options.servers');
    assert.number(opts.timeout, 'options.timeout');
    assert.func(cb, 'callback');

    assert.ok((opts.servers.length > 0), 'options.servers empty');
    for (var i = 0; i < opts.servers.length; i++) {
        assert.string(opts.servers[i].host, 'servers.host');
        assert.number(opts.servers[i].port, 'servers.port');
    }

    function _createClient(_, _cb) {
        var client = zkplus.createClient(opts);

        function onConnect() {
            client.removeListener('error', onError);
            log.info('zookeeper: connected');
            _cb(null, client);
        }

        function onError(err) {
            client.removeListener('connect', onConnect);
            _cb(err);
        }


        client.once('connect', onConnect);
        client.once('error', onError);

        client.connect();
    }

    var log = opts.log;
    var retry = backoff.call(_createClient, null, cb);
    retry.failAfter(Infinity);
    retry.setStrategy(new backoff.ExponentialStrategy({
        initialDelay: 1000,
        maxDelay: 30000
    }));

    retry.on('backoff', function (number, delay) {
        var level;
        if (number === 0) {
            level = 'info';
        } else if (number < 5) {
            level = 'warn';
        } else {
            level = 'error';
        }
        log[level]({
            attempt: number,
            delay: delay
        }, 'zookeeper: connection attempted (failed)');
    });
    retry.start();

    return (retry);
}


//
// Returns the current DB peer topology as a list of URLs, sorted in descending
// order: that is, '0' is always primary, '1' is always sync, and '2+' are
// async slaves
//
function _topology(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.string(opts.path, 'options.path');
    assert.object(opts.zk, 'options.zk');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = opts.log;
    var zk = opts.zk;

    log.debug({path: opts.path}, 'topology: entered');
    zk.readdir(opts.path, function (err, children) {
        if (err) {
            log.debug(err, 'topology: failed');

            if (err.code !== zkplus.ZNONODE) {
                cb(err);
            } else {
                setTimeout(function () {
                    _topology(opts, cb);
                }, 1000);
            }
            return;
        }

        var urls = childrenToURLs(children);
        log.debug({urls: urls}, 'topology: done');
        cb(null, urls, children);
    });
}


//
// Transforms a list of pg urls (tcp://pg@1.2.3.4/moray)
// into a hash:
//
// {
//   'primary': 'tcp://...',
//   'sync': 'tcp://...',
//   'async': 'tcp://...'
// }
//
function urlsToObject(urls) {
    var obj = {};
    var role;

    for (var i = 0; i < Math.min(urls.length, 3); i++) {
        if (i === 0) {
            role = 'primary';
        } else if (i === 1) {
            role = 'sync';
        } else {
            role = 'async';
        }

        obj[role] = urls[i];
    }

    return (obj);
}



////--- API

function Manatee(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.path, 'options.path');
    assert.object(options.pg, 'options.pg');
    assert.object(options.zk, 'options.zk');

    var self = this;
    EventEmitter.call(this);

    this.log = options.log.child({
        component: 'manatee',
        path: options.path,
        serializers: SERIALIZERS
    });
    this.path = options.path;
    this.pgConfig = options.pg;
    this.timeout = parseInt((options.timeout || 60000), 10);
    this._whatami = 'Manatee';
    this.pgUser = options.pg.user;
    if (options.pg.user) {
        PG_FMT = 'tcp://' + options.pg.user + '@%s:5432/%s';
    }

    function reEmit(event) {
        if (self.log.debug()) {
            var args = slice.apply(arguments);
            args.unshift();
            self.log.debug('caught event %s: %j', event, args);
        }

        self.emit.apply(self, arguments);
    }

    (function setupZK() {
        var zkOpts = {
            log: options.log,
            servers: options.zk.servers,
            timeout: parseInt(options.zk.timeout || 30000, 10)
        };
        createZkClient(zkOpts, function (zk_err, zk) {
            if (zk_err) {
                self.emit('error', zk_err);
                return;
            }

            self.zk = zk;

            function reconnect() {
                self.zk = null;
                process.nextTick(setupZK);
            }

            self.zk.once('close', reconnect);
            self.zk.on('error', function onZKClientError(err) {
                self.log.error(err, 'ZooKeeper client error');
                self.zk.removeAllListeners('close');
                self.zk.close();
                reconnect();
            });
            self._init();
        });
    })();
}
util.inherits(Manatee, EventEmitter);


Manatee.prototype.pg = function pg(options, callback) {
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


Manatee.prototype.start = function start(opts, cb) {
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


Manatee.prototype.toString = function toString() {
    var str = '[object Manatee <';
    str += 'listening=' + (this.listener ? 'yes' : 'no') + ', ';
    str += sprintf('path=%s', this.path);
    str += sprintf('servers=%j, ', this.zk.servers);
    str += sprintf('timeout=%d', this.timeout);
    str += '>]';

    return (str);
};



///--- Private Methods

//
// Reads the topology from ZK, and creates the initial
// DB pool
//
Manatee.prototype._init = function _init() {
    var self = this;
    var log = this.log;
    var opts = {
        log: self.log,
        path: self.path,
        zk: self.zk
    };

    log.debug('init: entered');
    _topology(opts, function (err, urls, children) {
        if (err) {
            log.fatal(err, 'init: error reading from zookeeper');
            throw new VError(err, 'init: error reading from ZK');
        } else if (!urls || !urls.length) {
            log.error('init: no DB shards available');
            self._watch();
            return;
        }

        self._refresh(children);
        process.nextTick(self.emit.bind(self, 'ready'));
    });
};


Manatee.prototype._watch = function _watch() {
    var log = this.log;
    var self = this;
    var zk = this.zk;

    log.debug('watch: entered');
    zk.watch(this.path, {method: 'list'}, function (werr, listener) {
        if (werr) {
            log.fatal(werr, 'watch: failed');
            self.emit('error', werr);
            return;
        }

        listener.once('error', function (err) {
            log.fatal(err, 'watch: error event fired; exiting');
            zk.emit('error', new VError(err, 'watch: unsolicited error event'));
        });

        listener.on('children', self._refresh.bind(self));
        log.debug('watch: started');
    });
};



// This is our workhorse for managing Postgres connections
// Whenever ZK sends a notification we need to actually see
// if topology changed (MANTA-1085), and if so shutdown the
// old connections and reconnect to the new leader.
//
// Topology change means that one of:
//
// - a new peer was added
// - a peer went away
// - a peer changed roles
//
// To do so, "db" is the current mapping, and topology is the
// new mapping.  For references, expect "db" to look like:
//
// {
//   primary: $PgPool.url = 'tcp://10.1.2.3',
//   sync:  $PgPool.url = 'tcp://10.1.2.4',
//   async:  $PgPool.url = 'tcp://10.1.2.5'
// }
//
// And the "pool" object will look the same, except the values
// will be just plain URL strings, not PGPool objects
//
Manatee.prototype._refresh = function _refresh(children) {
    var changes = false;
    var db = this.database || {};
    var log = this.log;
    var previous = {};
    var self = this;
    var topology = urlsToObject(childrenToURLs(children));

    // Redundant loops but we want to log before state
    // change
    Object.keys(db).forEach(function (k) {
        previous[k] = db[k].url;
    });

    log.debug({
        currentTopology: previous,
        newTopology: topology
    }, '_refresh: ZK children updated');

    // k is 'primary' et al, v is the url
    // First look for deletions:
    Object.keys(db).forEach(function (k) {
        if (!topology[k]) {
            log.debug({
                role: k,
                url: db[k].url
            }, '_refresh: peer gone (closing)');
            db[k].close();
            delete db[k];
            changes = true;
        }
    });

    // Then additions/changes
    Object.keys(topology).forEach(function (k) {
        var add = false;
        if (!db[k]) {
            log.debug({
                role: k,
                url: topology[k]
            }, '_refresh: new peer (adding)');
            add = true;
        } else if (db[k].url !== topology[k]) {
            log.debug({
                role: k,
                url: topology[k]
            }, '_refresh: peer role change (replace)');
            db[k].close();
            delete db[k];
            add = true;
        }

        if (add) {
            changes = true;
            db[k] = createPGPool({
                log: log,
                pgConfig: self.pgConfig,
                role: k,
                url: topology[k]
            });
        }
    });

    if (changes) {
        self.database = db;

        log.info({
            previousTopology: previous,
            topology: topology
        }, '_refresh: topology change');

        self.emit('topology', topology);
    } else {
        log.debug({
            previousTopology: previous,
            topology: topology
        }, '_refresh: topology had no impact');
    }
};




///--- Exports

module.exports = {
    createClient: function createClient(options) {
        return (new Manatee(options));
    }
};
