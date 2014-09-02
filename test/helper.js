/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var domain = require('domain');
var bunyan = require('bunyan');
var deepEqual = require('deep-equal');
var moray = require('moray'); // client
var once = require('once');



///--- Exports

module.exports = {

    after: function after(teardown) {
        module.parent.exports.tearDown = function _teardown(callback) {
            var d = domain.create();
            var self = this;
            d.on('error', function (err) {
                console.error('after: uncaught error: %s', err.stack);
                process.exit(1);
            });
            d.run(function () {
                teardown.call(self, once(callback));
            });
        };
    },

    before: function before(setup) {
        module.parent.exports.setUp = function _setup(callback) {
            var d = domain.create();
            var self = this;
            d.on('error', function (err) {
                console.error('before: uncaught error: %s', err.stack);
                process.exit(1);
            });
            d.run(function () {
                setup.call(self, once(callback));
            });
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
            port: (parseInt(process.env.MORAY_PORT, 10) || 2020),
            log: module.exports.createLogger()
        });
        return (client);
    }

};
