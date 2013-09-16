// Copyright 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var libuuid = require('libuuid');
var moray = require('moray');



///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: 'moray_load_test',
        stream: process.stdout,
        src: true,
        serializers: bunyan.stdSerializers
});

var CLIENT = moray.createClient({
        host: (process.env.MORAY_IP || '127.0.0.1'),
        port: (process.env.MORAY_PORT || 2020),
        log: LOG
});
CLIENT.once('connect', main);

var BNAME = 'moray_load_test';
var SCHEMA = {
        foo: { type: 'string' },
        bar: { type: 'string', unique: true }
};



///--- Runners

function put(i, cb) {
        var k = libuuid.create().substr(0, 7);
        var v = {
                foo: libuuid.create(),
                bar: libuuid.create()
        };
        CLIENT.putObject(BNAME, k, v, function (put_err, meta) {
                if (put_err) {
                        LOG.warn(put_err, 'putObject: failed');
                        put(++i, cb);
                        return;
                }

                CLIENT.getObject(BNAME, k, meta, function (get_err) {
                        if (get_err)
                                LOG.warn(get_err, 'getObject: failed');

                        put(++i, cb);
                });
        });
}




///--- Mainline
// Invoked only once the client is connected

function main() {
        CLIENT.putBucket(BNAME, SCHEMA, function (b_err) {
                assert.ifError(b_err);

                var done = 0;
                function cb() {
                        if (++done === 250)
                                main();
                }

                for (var i = 0; i < 250; i++)
                        put(1, cb);
        });
}