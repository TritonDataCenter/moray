// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');

var buckets = require('./buckets');
var manatee = require('./manatee');
var objects = require('./objects');
var sql = require('./sql');



///--- API

function createServer(options) {
        assert.object(options, 'options');

        var log = options.log;
        var db = manatee.createClient(options.manatee);
        db.once('ready', function () {
                db.removeAllListeners('error');

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

                server.on('after', function (msg) {
                        log.info({
                                method: msg.data.m.name,
                                responseTime: msg.elapsed / 1000
                        }, 'request handled');
                });

                server.on('error', function (err) {
                        log.error(err, 'server error');
                        process.exit(1);
                });

                function waitForPostgres() {
                        db.pg(function (err, client) {
                                if (err || !client) {
                                        log.error(err,
                                                  'Unable to talk to postgres');
                                        setTimeout(waitForPostgres, 2000);
                                        return;
                                }

                                client.rollback();
                                server.listen(options.port, function () {
                                        log.info('moray listening on %d',
                                                 options.port);
                                });

                                return;
                        });
                }

                waitForPostgres();
        });
        db.once('error', function (err) {
                log.error(err, 'Manatee: unable to load ZK topology');
                db.removeAllListeners('connect');
                db.removeAllListeners('ready');
                setTimeout(createServer.bind(null, options), 1000);
        });
}



///--- Exports

module.exports = {
        createServer: createServer
};
