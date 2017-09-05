/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var util = require('util');
var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var libuuid = require('libuuid');
var mod_fast = require('fast');
var mod_kang = require('kang');
var mod_net = require('net');
var mod_os = require('os');
var VError = require('verror').VError;
var LRU = require('lru-cache');
var vasync = require('vasync');

var control = require('./control');
var buckets = require('./buckets');
var objects = require('./objects');
var ping = require('./ping');
var sql = require('./sql');


///--- Globals

/*
 * API version, increment for depended-upon changes.
 *
 * This should not be incremented lightly: Moray consumers don't currently
 * have a great way to deal with Moray backends of different versions. Work
 * will need to be done there before this can be bumped. See RFD 33 for a
 * longer discussion on this issue.
 */
var API_VERSION = 2;

/*
 * This is the Kang version identifier. For now, we set it to 1.0.0. Someday,
 * we'll hopefully have a better way of grabbing information about the image
 * version and exporting that here.
 */
var KANG_VERSION = '1.0.0';


///--- API


function MorayServer(options) {
    var self = this;

    EventEmitter.call(this);

    var log = options.log;
    var db;
    if (options.standalone) {
        options.standalone.log = log;
        db = require('./standalone').createClient(options.standalone);
    } else {
        options.manatee.log = log;
        db = require('./manatee').createClient(options.manatee);
    }

    this.ms_bucketCache = new LRU({
        name: 'BucketCache',
        max: 100,
        maxAge: (300 * 1000)
    });
    this.ms_objectCache = new LRU({
        name: 'ObjectCache',
        max: 1000,
        maxAge: (30 * 1000)
    });
    var opts = {
        log: options.log,
        manatee: db,
        bucketCache: this.ms_bucketCache,
        objectCache: this.ms_objectCache
    };

    var socket = mod_net.createServer({ 'allowHalfOpen': true });
    var server = new mod_fast.FastServer({
        log: log.child({ component: 'fast' }),
        server: socket
    });

    var methods = [
        { rpcmethod: 'createBucket', rpchandler: buckets.creat(opts) },
        { rpcmethod: 'getBucket', rpchandler: buckets.get(opts) },
        { rpcmethod: 'listBuckets', rpchandler: buckets.list(opts) },
        { rpcmethod: 'updateBucket', rpchandler: buckets.update(opts) },
        { rpcmethod: 'delBucket', rpchandler: buckets.del(opts) },
        { rpcmethod: 'putObject', rpchandler: objects.put(opts) },
        { rpcmethod: 'batch', rpchandler: objects.batch(opts) },
        { rpcmethod: 'getObject', rpchandler: objects.get(opts) },
        { rpcmethod: 'delObject', rpchandler: objects.del(opts) },
        { rpcmethod: 'findObjects', rpchandler: objects.find(opts) },
        { rpcmethod: 'updateObjects', rpchandler: objects.update(opts) },
        { rpcmethod: 'reindexObjects', rpchandler: objects.reindex(opts) },
        { rpcmethod: 'deleteMany', rpchandler: objects.deleteMany(opts) },
        { rpcmethod: 'getTokens', rpchandler: getTokens(opts) },
        { rpcmethod: 'sql', rpchandler: sql.sql(opts) },
        { rpcmethod: 'ping', rpchandler: ping.ping(opts) },
        { rpcmethod: 'version', rpchandler: ping.version(opts, API_VERSION) }
    ];

    methods.forEach(function (rpc) {
        server.registerRpcMethod(rpc);
    });

    this.port = options.port;
    this.ip = options.bindip;

    this.fast_socket = socket;
    this.fast_server = server;
    this.kang_server = null;
    this.db_conn = db;
    this.log = options.log;

    if (options.kangPort) {
        mod_kang.knStartServer({
            port: options.kangPort,
            host: options.bindip,
            uri_base: '/kang',
            service_name: 'moray',
            version: KANG_VERSION,
            ident: mod_os.hostname() + '/' + process.pid,
            list_types: server.kangListTypes.bind(server),
            list_objects: server.kangListObjects.bind(server),
            get: server.kangGetObject.bind(server),
            stats: server.kangStats.bind(server)
        }, function (err, kang) {
            if (err) {
                self.log.error(err, 'failed to start Kang');
            } else {
                self.kang_server = kang;
            }
        });
    }


    /*
     * Once both the server socket and the database connection are up,
     * emit the "ready" event to indicate that the database is good to
     * use now.
     *
     * Failure to listen() will be re-emitted on the MorayServer. In a
     * "moray" zone, no one is listening, so the error will be thrown,
     * and SMF will either restart us or take the service into
     * maintenance.
     */
    var sock_ready = false;
    var dbc_ready = false;

    function emitReady() {
        if (sock_ready && dbc_ready) {
            self.emit('ready');
        }
    }

    self.fast_socket.on('listening', function () {
        self.log.info({ address: self.fast_socket.address() },
            'Moray listening on port %d', self.port);
        sock_ready = true;
        emitReady();
    });

    self.fast_socket.on('error', function (err) {
        self.fast_socket = null;
        self.emit('error', err);
    });

    self.db_conn.on('ready', function () {
        dbc_ready = true;
        emitReady();
    });
}
util.inherits(MorayServer, EventEmitter);


MorayServer.prototype.listen = function listen() {
    this.fast_socket.listen(this.port, this.ip);
};


MorayServer.prototype.close = function close() {
    var barrier = vasync.barrier();
    var self = this;

    /*
     * Wait until both the server socket and the DB connection
     * have been shutdown before closing Fast and Kang.
     */
    barrier.on('drain', function () {
        self.fast_server.close();

        if (self.kang_server !== null) {
            self.kang_server.close();
        }

        self.emit('close');
    });

    if (self.fast_socket !== null) {
        barrier.start('close-server-socket');
        self.fast_socket.on('close', function () {
            barrier.done('close-server-socket');
        });
        self.fast_socket.close();
    }

    barrier.start('close-db-conn');
    self.db_conn.on('close', function () {
        barrier.done('close-db-conn');
    });
    self.db_conn.close();
};


function createServer(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    return new MorayServer(options);
}



///--- Exports

module.exports = {
    createServer: createServer
};



///--- Privates

var GET_TOKENS_ARGS = [
    { name: 'options', type: 'options' }
];

function getTokens(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _getTokens(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, GET_TOKENS_ARGS)) {
            return;
        }

        var opts = argv[0];
        var id = opts.req_id || libuuid.create();
        var log = options.log.child({
            req_id: id
        });

        log.debug('getTokens: entered');

        rpc.fail(new Error('Operation not supported'));
    }

    return _getTokens;
}
