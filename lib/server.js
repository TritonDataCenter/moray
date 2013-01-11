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
        var server = fast.createServer({
                log: log
        });

        server.rpc('createBucket', buckets.creat({
                log: options.log,
                manatee: db
        }));

        server.rpc('getBucket', buckets.get({
                log: options.log,
                manatee: db
        }));

        server.rpc('updateBucket', buckets.update({
                log: options.log,
                manatee: db
        }));

        server.rpc('delBucket', buckets.del({
                log: options.log,
                manatee: db
        }));

        server.rpc('putObject', objects.put({
                log: options.log,
                manatee: db
        }));

        server.rpc('getObject', objects.get({
                log: options.log,
                manatee: db
        }));

        server.rpc('delObject', objects.del({
                log: options.log,
                manatee: db
        }));

        server.rpc('findObjects', objects.find({
                log: options.log,
                manatee: db
        }));

        server.rpc('sql', sql.sql({
                log: options.log,
                manatee: db
        }));

        server.rpc('ping', ping.ping({
                log: options.log,
                manatee: db
        }));

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
