// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var uuid = require('node-uuid');
var xtend = require('xtend');
var vasync = require('vasync');
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


// manatee writes something like: 10.99.99.14-123456
// sort by the number after the -, and you'll have:
// [primary, sync, async]
//
function createPGClients(opts) {
        assert.object(opts, 'options');
        assert.object(opts.log, 'options.log');
        assert.object(opts.pgConfig, 'options.pgConfig');

        var log = opts.log;
        var pool = urlsToObject(opts.urls);
        pool._urlMap = {};

        log.debug({urls: opts.urls}, 'createPgClients: entered');

        Object.keys(pool).forEach(function (k) {
                if (typeof (pool[k]) !== 'string')
                        return;

                var u = pool[k];
                pool[k] = postgresPool.createClient({
                        log: log,
                        checkInterval: opts.pgConfig.checkInterval,
                        connectTimeout: opts.pgConfig.connectTimeout,
                        maxConnections: opts.pgConfig.maxConnections,
                        maxIdleTime: opts.pgConfig.maxIdleTime,
                        queryTimeout: opts.pgConfig.queryTimeout,
                        role: k,
                        url: u
                });
                pool._urlMap[u] = pool[k];
        });

        log.debug({pool: pool}, 'createPgClients: done');
        return (pool);
}


function init(opts, cb) {
        assert.object(opts, 'options');
        assert.object(opts.pgConfig, 'options.pgConfig');
        assert.func(cb, 'callback');

        var log = opts.log;

        log.debug('init: entered');
        topology(opts, function (err, urls) {
                if (err) {
                        cb(err);
                } else {
                        log.debug('init: topology done. Creating PG clients');
                        var pool = createPGClients({
                                log: log,
                                pgConfig: opts.pgConfig,
                                urls: urls
                        });

                        cb(null, pool);
                }
        });
}


function primaryOrSync() {
        var rand = Math.floor(Math.random() * (1 - 1000 + 1) + 1);
        return (rand % 2 === 0 ? 'primary' : 'sync');
}


function topology(opts, cb) {
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var log = opts.log;
        var zk = opts.zk;

        log.debug({path: opts.path}, 'topology: entered');
        zk.readdir(opts.path, function (err, children) {
                if (err) {
                        log.debug(err, 'topology: failed');

                        if (err.code !== zkplus.ZNONODE) {
                                cb(err);
                        } else {
                                setTimeout(topology.bind(null, opts, cb), 1000);
                        }
                } else {
                        var urls = childrenToURLs(children);
                        log.debug({urls: urls}, 'topology: done');
                        cb(null, urls);
                }
        });
}


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


function watch(opts, cb) {
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var log = opts.log;
        var manatee = opts.manatee;
        var pgPool = opts.pgPool;
        var zk = opts.zk;

        log.debug('watch: entered');
        zk.watch(opts.path, {method: 'list'}, function (err, listener) {
                if (err) {
                        log.fatal(err, 'watch: failed');
                        return (cb(err));
                }

                listener.once('error', function (err2) {
                        log.fatal(err2, 'watch: error event fired; bailing');
                        process.exit(1);
                        // TODO: handle this better
                        // listener.stop();
                        // manatee.emit('error', err);
                        // watch(opts);
                });

                listener.on('children', function onChildren(children) {
                        var urls = childrenToURLs(children);
                        var pool = urlsToObject(urls);

                        var t = {};
                        pgPool.primary = null;
                        pgPool.sync = null;
                        pgPool.async = null;

                        Object.keys(pool).forEach(function (k) {
                                t[k] = pool[k];
                                pgPool[k] = pgPool._urlMap[pool[k]];
                        });

                        log.info({
                                children: children,
                                pool: pgPool,
                                urls: urls
                        }, 'watch: topology change');
                        manatee.emit('topology', t);
                });

                log.debug('watch: started');
                return (cb());
        });
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
        this.timeout = parseInt((options.timeout || 10000), 10);
        this.zk = zkplus.createClient({
                connectTimeout: options.zk.connectTimeout,
                log: options.log,
                servers: options.zk.servers,
                timeout: parseInt(options.zk.timeout || 10000, 10)
        });

        var _opts = {
                log: self.log,
                manatee: self,
                path: self.path,
                pgConfig: options.pg,
                zk: self.zk
        };

        this.zk.once('connect', function onConnect() {
                init(_opts, function (err, db) {
                        _opts.pgPool = db;
                        watch(_opts, function (err2) {
                                if (err2) {
                                        self.emit('error', err2);
                                } else {
                                        self.database = db;
                                        self.emit('ready');
                                }
                        });
                });
        });

        function reEmit(event) {
                if (this.log.debug()) {
                        var args = slice.apply(arguments);
                        args.unshift();
                        this.log.debug('manatee event %s: %j', event, args);
                }

                this.emit.apply(this, arguments);
        }

        this.zk.once('close', reEmit.bind(this, 'close'));
        this.zk.once('error', reEmit.bind(this, 'error'));
}
util.inherits(Manatee, EventEmitter);


Manatee.prototype.pg = function pg(options, callback) {
        if (typeof (options) === 'function') {
                callback = options;
                options = {};
        }
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        var db;
        var log = this.log;
        var peer;

        if (!this.database || !this.database.primary) {
                process.nextTick(function () {
                        callback(new NoDatabaseError());
                });
                return (undefined);
        }

        if (options.async) {
                peer = 'async';
        } else if (options.read) {
                peer = primaryOrSync();
        }
        peer = peer || 'primary';
        db = this.database[peer] || this.database.primary;

        log.debug({
                db: db,
                options: options,
                peer: peer
        }, 'pg: entered');
        db.checkout(function pgCallback(err, client) {
                if (err) {
                        log.debug(err, 'pg: failed');
                        if (client)
                                db.release(client);
                        return (callback(err));
                }

                if (!options.start) {
                        log.debug({pg: client}, 'pg: done');
                        return (callback(null, client));
                }

                log.debug({pg: client}, 'pg: starting transaction');
                client.begin(function (txnErr) {
                        if (txnErr) {
                                log.debug({
                                        pg: client,
                                        err: txnErr
                                }, 'pg: BEGIN failed');
                                db.release(client);
                                callback(txnErr);
                        } else {
                                log.debug({
                                        pg: client
                                }, 'pg: BEGIN done (all done)');
                                callback(null, client);
                        }
                });
                return (undefined);
        });
        return (undefined);
};


Manatee.prototype.start = function start(opts, cb) {
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }

        this.pg({
                async: opts.async,
                read: opts.read,
                start: true
        }, cb);
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



///--- Exports

module.exports = {
        createClient: function createClient(options) {
                return (new Manatee(options));
        }
};



///--- Tests
// var bunyan = require('bunyan');

// assert.ok(process.env.ZK_IP, 'set ZK_IP');

// var client = new Manatee({
//         path: '/manatee/3.moray.bh1-kvm1.joyent.us',
//         log: bunyan.createLogger({
//                 name: 'manatee',
//                 level: 'debug',
//                 stream: process.stdout
//         }),
//         pg: {
//                 connectTimeout: 1000,
//                 maxConnections: 5,
//                 maxIdleTime: 30000,
//                 checkInterval: 10000,
//                 queryTimeout: 1000
//         },
//         zk: {
//                 servers: [{
//                         host: process.env.ZK_IP,
//                         port: 2181
//                 }],
//                 connectTimeout: 1000,
//                 timeout: 2000
//         }
// });

// client.on('ready', function () {
//         client.pg({start: true}, function (err, pg) {
//                 pg.rollback();
//                 client.close(function () {
//                         console.log('test ok');
//                 });
//         });
// });
