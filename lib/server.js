// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');

var buckets = require('./buckets');
var objects = require('./objects');
var pg = require('./pg');



///--- API

function createServer(options) {
        assert.object(options, 'options');

        // TODO: determine based on ZK and headers
        options.postgres.zk = {};
        var log = options.log;
        var pgPool = pg.createClient(options.postgres);
        var server = fast.createServer();

        server.rpc('createBucket', buckets.creat({
                log: options.log,
                pg: pgPool
        }));

        server.rpc('getBucket', buckets.get({
                log: options.log,
                pg: pgPool
        }));

        server.rpc('delBucket', buckets.del({
                log: options.log,
                pg: pgPool
        }));

        server.rpc('putObject', objects.put({
                log: options.log,
                pg: pgPool
        }));

        server.rpc('getObject', objects.get({
                log: options.log,
                pg: pgPool
        }));

        server.rpc('delObject', objects.del({
                log: options.log,
                pg: pgPool
        }));

        server.rpc('findObjects', objects.find({
                log: options.log,
                pg: pgPool
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
