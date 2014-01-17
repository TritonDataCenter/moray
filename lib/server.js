// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');
var libuuid = require('libuuid');
var VError = require('verror').VError;

var buckets = require('./buckets');
var manatee = require('./manatee');
var standalone = require('./standalone');
var objects = require('./objects');
var ping = require('./ping');
var sql = require('./sql');



///--- Globals

var slice = Function.prototype.call.bind(Array.prototype.slice);



///--- API

function createServer(options) {
    assert.object(options, 'options');

    var log = options.log;
    var db;
    if (options.standalone) {
        db = standalone.createClient(options.standalone);
    } else {
        db = manatee.createClient(options.manatee);
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
    server.rpc('deleteMany', objects.deleteMany(opts));
    server.rpc('getTokens', getTokens(opts));
    server.rpc('sql', sql.sql(opts));
    server.rpc('ping', ping.ping(opts));

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

    server.listen(options.port, options.bindip, function () {
        log.info('moray listening on %d', options.port);
    });
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

        log.debug({
            opts: opts,
            res: res
        }, 'getTokens: entered');

        res.end(new Error('Operation not supported'));
    }

    return _getTokens;
}
