/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/**
 * This uses a persistent moray connection to listen for postgres notifications.
 * See PostgreSQL notify: https://www.postgresql.org/docs/9.0/sql-notify.html
 */

var assert = require('assert-plus');
var once = require('once');
var vasync = require('vasync');

var control = require('./control');
var pgError = require('./pg').pgError;


// --- Globals

var LISTEN_ARGS_SCHEMA = [
    { name: 'channel', type: 'string' },
    { name: 'options', type: 'options' }
];

var UNLISTEN_ARGS_SCHEMA = [
    { name: 'id', type: 'string' },
    { name: 'options', type: 'options' }
];

/**
 * The moray server will keep track of the listeners it has created (keyed from
 * the listen request id). This is so the listener can later be stopped through
 * the unlisten RPC.
 */
var unlistenCallbackHandlers = {};


// --- Handlers

/**
 * listen(CHANNEL, OPTS)
 *  - CHANNEL {String} the notify message channel to listen for
 *  - OPTS {Object} RPC request options
 *
 * This listen call is a long-running streaming RPC call.
 *
 * After a successful listen, postgres 'notification' messages will be streamed
 * back to the client. The client must invoke the *unlisten* (using the request
 * id) RPC call in order to stop receiving 'notification' messages and allow
 * the listen standalone postgres connection to be closed.
 */
function listen(options) {
    control.assertOptions(options);

    function _listen(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, LISTEN_ARGS_SCHEMA)) {
            return;
        }

        var channel = argv[0];
        var opts = argv[1];
        var req = control.buildReq(opts, rpc, options);
        var log = req.log;

        req.channel = channel;

        log.debug({channel: channel, opts: opts}, 'listen');

        // Add a handler for when the RPC client socket ends.
        function onClientSocketEnd() {
            rpc.clientSocketEnded = true;
            // Fire the callback handler when it's registered.
            var callback = unlistenCallbackHandlers[req.req_id];
            if (callback) {
                callback();
            }
        }

        rpc.addSocketEndListener(onClientSocketEnd);

        vasync.pipeline({arg: req, funcs: [
            createStandalonePgClient,
            listenAndNotify
        ]}, function _onListenPipelineCb(err) {
            // Remove callback handler.
            rpc.removeSocketEndListener(onClientSocketEnd);
            delete unlistenCallbackHandlers[req.req_id];
            // Close standalone pg connection.
            if (req.pg) {
                req.pg.release();
                req.pg = null;
            }
            // Close RPC.
            if (err) {
                log.debug(err, 'listen: failed');
                rpc.fail(pgError(err));
            } else {
                log.debug('listen: done');
                rpc.end();
            }
        });
    }

    return (_listen);
}

/**
 * unlisten(REQ_ID, OPTS)
 *  - REQ_ID {String} the original RPC listen request id
 *  - OPTS {Object} RPC request options
 *
 * This will unregister a previous listen call, to stop receiving 'notification'
 * messages.
 */
function unlisten(options) {
    control.assertOptions(options);

    function _unlisten(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, UNLISTEN_ARGS_SCHEMA)) {
            return;
        }

        var id = argv[0];
        // var opts = argv[1]; // Unused (RFU)
        var log = options.log;

        log.debug({id: id}, 'unlisten');

        // Fire the listener end callback handler.
        var callback = unlistenCallbackHandlers[id];
        if (callback) {
            log.debug({id: id}, 'unlisten: invoking callback');
            callback();
        }

        rpc.end();
    }

    return (_unlisten);
}


// Internal helpers.

function createStandalonePgClient(req, callback) {
    var log = req.log;
    var rpc = req.rpc;

    if (rpc.clientSocketEnded) {
        log.debug('listen RPC client socket has already ended');
        callback();
        return;
    }

    req.manatee.connectStandalonePgClient(function (err, pg) {
        if (err) {
            log.debug(err, 'listen: no DB handle');
            callback(err);
            return;
        }
        pg.setRequestId(req.req_id);
        req.pg = pg;

        callback();
    });
}

function listenAndNotify(req, callback) {
    var channel = req.channel;
    var done;
    var log = req.log;
    var pg = req.pg;
    var rpc = req.rpc;

    if (rpc.clientSocketEnded) {
        log.debug('listen RPC client socket has already ended');
        callback();
        return;
    }

    function _done(err) {
        // Remove the unlisten callback func.
        pg.removeListener('error', done);
        pg.removeListener('end', done);
        pg.removeListener('close', done);

        callback(err);
    }

    done = once(_done);

    log.debug({
        channel: channel
    }, 'listenAndNotify: begin');

    pg.listen(channel);

    pg.on('notification', function (n) {
        /**
         * A typical notification looks like this:
         *  {
         *    name: 'notification',
         *    length: 300,
         *    processId: 67942,
         *    channel: '$channel',
         *    payload: '{"some": "data", ...}'
         *  }
         */
        assert.ok(n.payload, 'notification.payload');
        assert.equal(n.channel, channel);

        rpc.write({channel: n.channel, payload: n.payload});
    });

    pg.once('error', done);
    pg.once('end', done);
    pg.once('close', done);

    // Register a callback for when the client ends or wants to unlisten.
    unlistenCallbackHandlers[req.req_id] = done;
}


// --- Exports

module.exports = {
    listen: listen,
    unlisten: unlisten
};
