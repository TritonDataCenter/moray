// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var Cache = require('expiring-lru-cache');
var fast = require('fast');

var buckets = require('./buckets');
var objects = require('./objects');
// var common = require('./common');
var pg = require('./pg');



///--- Globals

var HOSTNAME = os.hostname();



///--- Internal Functions

function version() {
        var name = __dirname + '/../package.json';
        var pkg = fs.readFileSync(name, 'utf8');
        return (JSON.parse(pkg).version);
}



///--- API

function createServer(options) {
        assert.object(options, 'options');

        // TODO: determine based on ZK and headers
        options.postgres.zk = {};
        var pgPool = pg.createClient(options.postgres);
        var server = fast.createServer();

        server.rpc('creat', buckets.creat({
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

        var log = options.log;
        server.on('after', function (msg) {
                log.info({
                        method: msg.data.m.name,
                        responseTime: msg.elapsed / 1000
                }, 'request handled');
        });

        // server.get({
        //         name: 'GetObject',
        //         path:'/:bucket/:key'
        // }, objects.get());
        // server.put({
        //         name: 'PutObject',
        //         path:'/:bucket/:key'
        // }, objects.put());
        // server.head({
        //         name: 'HeadObject',
        //         path:'/:bucket/:key'
        // }, objects.get());


        // server.put('/:bucket', buckets.put());
        // server.get('/:bucket', buckets.get());
        // server.head('/:bucket', buckets.get());
        // server.del('/:bucket', buckets.del());

        // // If there were failures, we ensure rollback happens
        // server.on('after', function (req, res, route) {
        //         if (req.pg)
        //                 req.pgPool.rollback(req.pg, function () {});
        // });
        // server.on('after', restify.auditLogger({
        //         body: true,
        //         log: bunyan.createLogger({
        //                 name: 'audit',
        //                 streams: [ {
        //                         level: (process.env.LOG_LEVEL || 'info'),
        //                         stream: process.stdout
        //                 } ]
        //         })
        // }));

        // setInterval(function () {
        //         console.log('\n\navailable=%d\nmax=%d\nsize=%d\nwaiting=%d\n',
        //                     pgPool.pool.available.length,
        //                     pgPool.pool.max,
        //                     pgPool.pool.resources.length,
        //                     pgPool.pool.queue.length);
        //         console.log(require('util').inspect(process._getActiveRequests()));
        // }, 1000);


        return (server);
}



///--- Exports

module.exports = {

        createServer: createServer

};
