// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var uuid = require('node-uuid');
var xtend = require('xtend');
var vasync = require('vasync');
var zkplus = require('zkplus');

var pg_pool = require('./pg');



///--- Globals

var sprintf = util.format;

var DBNAME = process.env.MORAY_DB_NAME || 'moray';


///--- Internal Functions

function domainToPath(domain) {
        assert.ok(domain);
        return ('/' + domain.split('.').reverse().join('/'));
}


//-- The following are all private functions used by `watch` in a pipeline

function waitForPath(req, cb) {
        var log = req.log;

        log.debug('waitForPath: entered');
        req.zk.get(req.path, function (err, obj) {
                if (err) {
                        log.debug(err, 'waitForPath: failed');
                        if (err.code !== zkplus.ZNONODE)
                                return (cb(err));

                        return (setTimeout(function () {
                                waitForPath(req, cb);
                        }, 1000));
                }

                req.obj = obj;
                log.debug(obj, 'waitForPath: done');
                return (cb());
        });
}


function registerWatch(req, cb) {
        var log = req.log;

        log.debug('registerWatch: entered');
        req.zk.watch(req.path, {initialData: true}, function (err, listener) {
                if (err) {
                        log.error(err, 'registerWatch: failed');
                        return (cb(err));
                }

                log.debug('registerWatch: done');
                req.listener = listener;
                return (cb());
        });
}


function waitForChange(req, cb) {
        var listener = req.listener;
        var log = req.log;

        log.debug('waitForChange: entered');
        listener.once('data', function (obj) {
                log.debug(obj, 'waitForChange: done');
                req.record = obj.database;
                cb();
        });
}


function createPGPool(req, cb) {
        var log = req.log;
        var cfg = {
                log: log,
                checkInterval: req.pgConfig.checkInterval,
                maxConnections: req.pgConfig.maxConnections,
                maxIdleTime: req.pgConfig.maxIdleTime
        };
        var pgPool = {};

        log.debug('createPgPool: entered');

        // Manatee writes URLs like this:
        // tcp://postgres@10.2.201.84:5432/postgres
        // we want to strip off the /postgres
        function _create(name) {
                if (!req.record[name])
                        return (undefined);

                var url = req.record[name].replace(/\/postgres$/,
                                                   '\/' + DBNAME);

                log.debug({
                        url: url
                }, 'createPGPool: creating %s', name);
                pgPool[name] = pg_pool.createClient(xtend(cfg, {
                        url: url
                }));
                return (pgPool[name]);
        }

        _create('primary');
        _create('sync');
        _create('async');

        req.pgPool = pgPool;
        log.debug('createPgPool: done');
        cb();
}



////--- API

function Manatee(options) {
        assert.object(options, 'options');
        assert.string(options.domain, 'options.domain');
        assert.object(options.log, 'options.log');
        assert.object(options.pg, 'options.pg');
        assert.object(options.zk, 'options.zk');

        var self = this;
        EventEmitter.call(this);

        this.path = domainToPath(options.domain);
        this.log = options.log.child({
                clazz: 'Manatee',
                path: self.path,
                serializers: {
                        pg: function (c) {
                                return (c._moray_id);
                        },
                        err: bunyan.stdSerializers.err
                }
        });
        this.pgConfig = options.pg;
        this.servers = options.zk.servers;
        this.timeout = parseInt((options.timeout || 10000), 10);
        this.zk = zkplus.createClient({
                log: options.log,
                servers: self.servers,
                timeout: self.timeout
        });

        this.zk.on('connect', function () {
                self.watch();
                self.emit('connect');
        });
        this.zk.on('close', this.emit.bind(this, 'close'));
        this.zk.on('error', this.emit.bind(this, 'error'));
}
util.inherits(Manatee, EventEmitter);


Manatee.prototype.close = function close(options, callback) {
        if (typeof (options) === 'function') {
                callback = options;
                options = {};
        }
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        var log = this.log;
        var self = this;

        log.debug({
                path: self.path
        }, 'close: entered');

        if (this.listener)
                this.listener.stop();

        if (!options.skipZk)
                this.zk.close();


        var done = 0;
        function onClose() {
                if (++done >= keys.length) {
                        self.database = null;
                        log.debug('close: done');
                        callback();
                }
        }

        var keys = Object.keys(this.database || {});
        if (keys.length > 0) {
                keys.forEach(function (k) {
                        log.debug('closing: %s', k);
                        self.database[k].on('close', onClose);
                        self.database[k].close();
                });
        } else {
                onClose();
        }
};


Manatee.prototype.watch = function watch() {
        var log = this.log;
        var self = this;
        var req = {
                log: self.log,
                path: self.path,
                pgConfig: self.pgConfig,
                zk: self.zk
        };

        log.debug('watch: entered');

        vasync.pipeline({
                funcs: [
                        waitForPath,
                        registerWatch,
                        waitForChange,
                        createPGPool
                ],
                arg: req
        }, function (err) {
                if (err) {
                        log.error(err, 'watch: failed');
                        self.close(function () {});
                        self.emit('error', err);
                }
                req.listener.once('data', function (obj) {
                        log.debug(obj, 'watch: topology changed');
                        self.close({skipZk: true}, function () {
                                self.watch();
                        });
                });
                req.listener.once('error', function (err2) {
                        log.debug(err2, 'watch: listener emitted error');
                        self.close({skipZk: true}, function () {
                                self.watch();
                        });
                });
                log.debug('watch: closing previous db');
                self.close({skipZk: true}, function () {
                        self.database = req.pgPool;
                        self.listener = req.listener;
                        log.debug('watch: done (emitting ready)');
                        self.emit('ready');
                });
        });
};


Manatee.prototype.pg = function pg(options, callback) {
        if (typeof (options) === 'function') {
                callback = options;
                options = {};
        }
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        var db;
        var log = this.log;

        if (!this.database || !this.database.primary) {
                process.nextTick(function () {
                        callback(new NoDatabaseError());
                });
                return (undefined);
        }

        if (options.async) {
                db = this.database.async;
        } else if (options.read) {
                db = this.database.standby;
        }
        db = db || this.database.primary;

        log.debug({options: options}, 'pg: entered');
        db.checkout(function pgCallback(err, client) {
                if (err) {
                        log.debug(err, 'pg: failed');
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
                                client.release();
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


Manatee.prototype.start = function start(cb) {
        this.pg({start: true}, cb);
};


Manatee.prototype.toString = function toString() {
        var str = '[object Manatee <';
        str += 'listening=' + (this.listener ? 'yes' : 'no') + ', ';
        str += sprintf('path=%s', this.path);
        str += sprintf('servers=%j, ', this.servers);
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
// var client = new Manatee({
//         domain: 'pg.1.moray.bh1-kvm1.joyent.us',
//         log: bunyan.createLogger({
//                 name: 'manatee',
//                 level: 'debug',
//                 stream: process.stdout
//         }),
//         pg: {},
//         zk: {
//                 servers: [{
//                         host: (process.env.ZK_IP || '10.2.201.114'),
//                         port: 2181
//                 }],
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
