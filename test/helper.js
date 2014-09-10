/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var moray = require('moray'); // client

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



///--- Exports

module.exports = {
    createLogger: createLogger,
    createClient: createClient
};
