// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var restify = require('restify');

var buckets = require('./buckets');
var common = require('./common');
var objects = require('./objects');
var pg = require('./pg');



///--- Internal Functions

function formatJSON(req, res, body) {
        if (body instanceof Error) {
                // snoop for RestError or HttpError
                res.statusCode = body.statusCode || 500;

                body = body.body || { message: body.message };
        } else if (Buffer.isBuffer(body)) {
                body = body.toString('base64');
        }

        var data = JSON.stringify(body);
        res.setHeader('Content-Length', Buffer.byteLength(data));
        res.setHeader('Content-Type', 'application/json');

        return (data);
}


function version() {
        var name = __dirname + '/../package.json';
        var pkg = fs.readFileSync(name, 'utf8');
        return (JSON.parse(pkg).version);
}



///--- API

function createServer(options) {
        assert.object(options, 'options');

        // TODO: determine based on ZK and headers
        var pgPool = pg.createClient(options.postgres);
        var server = restify.createServer({
                formatters: {
                        'application/json': formatJSON
                },
                log: options.log,
                name: options.name,
                version: version()
        });
        server.pre(common.pause);
        server.use(restify.requestLogger());
        server.use(function debugLogRequest(req, res, next) {
                if (!req.log.debug())
                        return (next());

                var log = req.log;
                var str = req.method + ' ' +
                        req.url + ' ' +
                        req.httpVersion + '\n';
                Object.keys(req.headers).sort().forEach(function (k) {
                        str += k + ': ' + req.headers[k] + '\n';
                });
                log.debug('handling request:\n%s\n', str);
                return (next());
        });
        server.use(function setup(req, res, next) {
                req.config = options;
                req.pg = pgPool;
                res.once('header', function () {
                        var now = Date.now();
                        res.header('Date', new Date());
                        res.header('x-request-id', req.getId());
                        var t = now - req.time();
                        res.header('x-response-time', t);
                        res.header('x-server-name', os.hostname());
                });
                next();
        });

        // In moray, we omit most useful restify checks because we assume we're
        // being contacted by "good internal apps", which you know, is hopefully
        // the case.  Bad clients should just see unexpected behavior.  We do
        // this because we want moray to frickin' rip.  Every (milli|micro)second
        // counts at this tier.
        server.use(restify.queryParser());

        server.put({
                name: 'PutObject',
                path:'/:bucket/:key'
        }, objects.put());

        server.put('/:bucket', buckets.put());
        server.get('/:bucket', buckets.get());
        server.head('/:bucket', buckets.get());
        server.del('/:bucket', buckets.del());

        // If there were failures, we ensure rollback happens
        server.on('after', common.rollbackTransaction);
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
