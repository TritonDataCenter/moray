/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var control = require('./control');
var libuuid = require('libuuid');


///--- Globals

var ARGS_SCHEMA = [
    { name: 'options', type: 'options' }
];


///--- Handlers


function ping(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _ping(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var opts = argv[0];
        var log = options.log.child({
            req_id: opts.req_id || libuuid.create()
        });

        log.debug({
            opts: opts
        }, 'ping: entered');

        if (!opts.deep) {
            log.debug('ping: done');
            rpc.end();
            return;
        }

        manatee.pg(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'ping: no DB handle');
                rpc.fail(startErr);
                return;
            }

            var req = pg.query('SELECT name FROM buckets_config ' +
                               'LIMIT 1');

            req.once('error', function (err) {
                log.debug(err, 'postgres error');
                req.removeAllListeners('end');
                req.removeAllListeners('row');
                pg.release();
                rpc.fail(err);
            });

            req.once('row', function (r) {
                log.debug(r, 'row received');
            });

            req.once('end', function () {
                log.debug('request done');
                req.removeAllListeners('error');
                req.removeAllListeners('row');
                pg.release();
                rpc.end();
            });
        });

    }

    return (_ping);
}


function version(options, ver) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _version(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var opts = argv[0];
        var log = options.log.child({
            req_id: opts.req_id || libuuid.create()
        });
        log.debug({
            opts: opts
        }, 'version: entered');
        rpc.end({ version: ver });
        log.debug('version: done');
    }

    return (_version);
}


///--- Exports

module.exports = {
    ping: ping,
    version: version
};
