// Copyright 2012 Joyent, Inc.  All rights reserved.

var bunyan = require('bunyan');
var deepEqual = require('deep-equal');
var moray = require('moray'); // client



///--- Exports

module.exports = {

        after: function after(teardown) {
                module.parent.exports.tearDown = function _teardown(callback) {
                        try {
                                teardown.call(this, callback);
                        } catch (e) {
                                console.error('after:\n' + e.stack);
                                process.exit(1);
                        }
                };
        },

        before: function before(setup) {
                module.parent.exports.setUp = function _setup(callback) {
                        try {
                                setup.call(this, callback);
                        } catch (e) {
                                console.error('before:\n' + e.stack);
                                process.exit(1);
                        }
                };
        },

        test: function test(name, tester) {
                module.parent.exports[name] = function _(t) {
                        var _done = false;
                        t.end = function end() {
                                if (!_done) {
                                        _done = true;
                                        t.done();
                                }
                        };
                        t.notOk = function notOk(ok, message) {
                                return (t.ok(!ok, message));
                        };

                        tester.call(this, t);
                };
        },

        createLogger: function createLogger(name, stream) {
                var log = bunyan.createLogger({
                        level: (process.env.LOG_LEVEL || 'warn'),
                        name: name || process.argv[1],
                        stream: stream || process.stdout,
                        src: true,
                        serializers: bunyan.stdSerializers
                });
                return (log);
        },

        createClient: function createClient() {
                var client = moray.createClient({
                        host: (process.env.MORAY_IP || '127.0.0.1'),
                        port: (process.env.MORAY_PORT || 2020),
                        log: module.exports.createLogger()
                });
                return (client);
        }

};
