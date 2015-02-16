/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var util = require('util');
var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var fast = require('fast');
var libuuid = require('libuuid');
var VError = require('verror').VError;

var buckets = require('./buckets');
var objects = require('./objects');
var ping = require('./ping');
var sql = require('./sql');


///--- Globals

var slice = Function.prototype.call.bind(Array.prototype.slice);



///--- API


function MorayServer(options) {
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
    var opts = {
        log: options.log,
        manatee: db
    };


    var server = fast.createServer(opts);

    server.rpc('createBucket', buckets.creat(opts));
    server.rpc('getBucket', buckets.get(opts));
    server.rpc('listBuckets', buckets.list(opts));
    server.rpc('updateBucket', buckets.update(opts));
    server.rpc('delBucket', buckets.del(opts));
    server.rpc('putObject', objects.put(opts));
    server.rpc('batch', objects.batch(opts));
    server.rpc('getObject', objects.get(opts));
    server.rpc('delObject', objects.del(opts));
    server.rpc('findObjects', objects.find(opts));
    server.rpc('updateObjects', objects.update(opts));
    server.rpc('reindexObjects', objects.reindex(opts));
    server.rpc('deleteMany', objects.deleteMany(opts));
    server.rpc('getTokens', getTokens(opts));
    server.rpc('sql', sql.sql(opts));
    server.rpc('ping', ping.ping(opts));
    // API version, increment for depended-upon changes
    var API_VERSION = 2;
    server.rpc('version', ping.version(opts, API_VERSION));

    if (options.audit !== false) {
        server.on('after', function (name, req, res) {
            var t = Math.floor(res.elapsed / 1000);
            var obj = {
                method: name,
                'arguments': req,
                serverTime: t + 'ms'
            };

            log.info(obj, 'request handled');
        });
    }

    server.on('error', function (err) {
        log.error(err, 'server error');
        throw new VError(err, 'unsolicited server error');
    });
    server.on('uncaughtException', function (err) {
        log.error(err, 'uncaught: server error');
        var args = slice(arguments);
        args.shift();
        args[args.length - 1].end(err);
    });

    this.port = options.port;
    this.ip = options.bindip;

    this.fast_server = server;
    this.db_conn = db;
    this.log = options.log;

    var self = this;
    ['listening', 'error'].forEach(function (event) {
        // re-emit certain events from fast server
        self.fast_server.on(event, self.emit.bind(self, event));
    });
    ['ready'].forEach(function (event) {
        // re-emit certain events from manatee
        self.db_conn.on(event, self.emit.bind(self, event));
    });
}
util.inherits(MorayServer, EventEmitter);


MorayServer.prototype.listen = function listen() {
    var self = this;
    this.fast_server.listen(this.port, this.ip, function () {
        self.log.info('moray listening on %d', self.port);
    });
};


MorayServer.prototype.close = function close() {
    this.fast_server.close();
    this.db_conn.on('close', this.emit.bind(this, 'close'));
    this.db_conn.close();
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

function getTokens(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _getTokens(opts, res) {
        var id = opts.req_id || libuuid.create();
        var log = options.log.child({
            req_id: id
        });

        log.debug('getTokens: entered');

        res.end(new Error('Operation not supported'));
    }

    return _getTokens;
}
