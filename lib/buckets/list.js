/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var control = require('../control');
var dtrace = require('../dtrace');
var util = require('util');


///--- Globals

var ARGS_SCHEMA = [
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandle,
    loadBuckets
];


///--- Handlers

function loadBuckets(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var rows = [];
    var sql = 'SELECT * FROM buckets_config';

    log.debug('loadBuckets: entered');
    dtrace['loadbuckets-start'].fire(function () {
        return ([req.msgid, req.req_id]);
    });
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'loadBuckets: failed');
        dtrace['loadbuckets-error'].fire(function () {
            return ([req.msgid, req.req_id, sql]);
        });
        cb(err);
    });

    q.on('row', function (r) {
        dtrace['loadbuckets-row'].fire(function () {
            return ([req.msgid, req.req_id, r.name]);
        });
        r.options = r.options || {};
        r.options.version = r.options.version || 0;
        req.rpc.write(r);
    });

    q.once('end', function () {
        log.debug({
            buckets: rows
        }, 'loadBuckets: done');
        dtrace['loadbuckets-done'].fire(function () {
            return ([req.msgid, req.req_id]);
        });
        cb(null);
    });
}


function list(options) {
    control.assertOptions(options);

    function _list(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var opts = argv[0];

        var req = control.buildReq(opts, rpc, options);

        req.log.debug({
            opts: opts
        }, 'listBuckets: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE
        });
    }

    return (_list);
}



///--- Exports

module.exports = {
    list: list
};
