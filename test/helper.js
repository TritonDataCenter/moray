/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var fs = require('fs');

var bunyan = require('bunyan');
var moray = require('moray'); // client
var app = require('../lib');

///--- API

function createLogger(name, stream) {
    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: name || process.argv[1],
        stream: stream || process.stdout,
        src: true,
        serializers: bunyan.stdSerializers
    });
    return (log);
}

function createClient() {
    var client = moray.createClient({
        host: (process.env.MORAY_IP || '127.0.0.1'),
        port: (parseInt(process.env.MORAY_PORT, 10) || 2020),
        log: createLogger()
    });
    return (client);
}

function createServer(cb) {
    if (!process.env.MORAY_IP) {
        var configPath = process.env.MORAY_CONFIG ||
            __dirname + '/../etc/config.standalone.json';
        var config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        config.log = createLogger('moray-server');
        var server = app.createServer(config);
        server.once('ready', function () {
            server.once('listening', cb.bind(null, server));
            server.listen();
        });
    } else {
        cb(null);
    }
}

function cleanupServer(server, cb) {
    if (server) {
        server.once('close', cb);
        server.close();
    } else {
        cb();
    }
}
///--- Exports

module.exports = {
    createLogger: createLogger,
    createClient: createClient,
    createServer: createServer,
    cleanupServer: cleanupServer
};
