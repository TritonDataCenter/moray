// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');

var buckets = require('./buckets');
var manatee = require('./manatee');
var objects = require('./objects');
var ping = require('./ping');
var sql = require('./sql');



///--- API

function createServer(options) {
        assert.object(options, 'options');

        var log = options.log;
        var db = manatee.createClient(options.manatee);
        var opts = {
                log: options.log,
                manatee: db
        };
        var server = fast.createServer(opts);

        server.rpc('createBucket', buckets.creat(opts));
        server.rpc('getBucket', buckets.get(opts));
        server.rpc('updateBucket', buckets.update(opts));
        server.rpc('delBucket', buckets.del(opts));
        server.rpc('putObject', objects.put(opts));
        server.rpc('batchPutObject', objects.batchPut(opts));
        server.rpc('getObject', objects.get(opts));
        server.rpc('delObject', objects.del(opts));
        server.rpc('findObjects', objects.find(opts));
        server.rpc('updateObjects', objects.update(opts));
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
                process.exit(1);
        });

        server.listen(options.port, function () {
                log.info('moray listening on %d', options.port);
        });
}



///--- Exports

module.exports = {
        createServer: createServer
};
