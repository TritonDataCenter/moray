/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert');
var bunyan = require('bunyan');
var cluster = require('cluster');
var moray = require('moray');

assert.ok(process.env.MORAY_URL, 'specify MORAY_URL');

var numCPUs = require('os').cpus().length;
if (cluster.isMaster) {
        // Fork workers.
        for (var i = 0; i < numCPUs; i++) {
                cluster.fork();
        }

        cluster.on('exit', function (worker, code, signal) {
                console.log('worker ' + worker.process.pid + ' died');
        });
} else {
        var client = moray.createClient({
                log: bunyan.createLogger({
                        level: 'warn',
                        name: 'manta1027',
                        stream: process.stdout,
                        serializers: bunyan.stdSerializers
                }),
                url: process.env.MORAY_URL
        });

        client.once('connect', function () {
                function put() {
                        client.putObject('manta', '1027', {}, function (err) {
                                if (err) {
                                        console.error(err.stack);
                                }
                                put(i);
                        });
                }

                var max = parseInt(process.env.MANTA1027_CONCURRENCY || 20, 10);
                console.log('running...');
                for (var ii = 0; ii < max; ii++) {
                        put();
                }
        });
}
