// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');

var buckets = require('./buckets');
var manatee = require('./manatee');
var objects = require('./objects');



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

        server.on('after', function (msg) {
                log.info({
                        method: msg.data.m.name,
                        responseTime: msg.elapsed / 1000
                }, 'request handled');
        });

        return (server);
}



///--- Exports

module.exports = {
        createServer: createServer
};
