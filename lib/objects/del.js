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

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');

var mod_errors = require('../errors');
var ObjectNotFoundError = mod_errors.ObjectNotFoundError;
var EtagConflictError = mod_errors.EtagConflictError;


///--- Globals

var ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'key', type: 'string' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    common.loadBucket,
    drop,
    common.runPostChain
];

// This is exported for batch 'delete' operations
var SUBPIPELINE = PIPELINE.slice(1);

///--- Handlers

function drop(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    var sql = util.format(('DELETE FROM %s WHERE _key=$1 ' +
                           'RETURNING _id, _etag, \'%s\' AS req_id'),
                          req.bucket.name, req.req_id);

    log.debug({
        bucket: req.bucket.name,
        key: req.key,
        sql: sql
    }, 'drop: entered');

    q = pg.query(sql, [req.key]);

    q.once('error', function (err) {
        log.debug(err, 'drop: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        if (!row) {
            log.debug('drop: record did not exist');
            cb(new ObjectNotFoundError(req.bucket.name, req.key));
        } else if (req.etag && row._etag !== req.etag) {
            log.debug('drop: etag conflict');
            cb(new EtagConflictError(req.bucket.name,
                                     req.key,
                                     req.etag,
                                     row._etag));
        } else {
            req._id = row._id;
            assert.ok(req._id);
            log.debug({id: req._id}, 'drop: done');
            cb();
        }
    });
}


function del(options) {
    control.assertOptions(options);

    function _del(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var bucket = argv[0];
        var key = argv[1];
        var opts = argv[2];

        var req = control.buildReq(opts, rpc, options);
        req.bucket = {
            name: bucket
        };
        req.key = key;
        req.etag = (opts.etag || opts._etag);

        dtrace['delobject-start'].fire(function () {
            return ([req.msgid, req.req_id, bucket, key]);
        });
        req.log.debug({
            bucket: bucket,
            key: key,
            opts: opts
        }, 'delObject: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: common.stdOutput(req)
        });
    }

    return (_del);
}



///--- Exports

module.exports = {
    del: del,
    pipeline: SUBPIPELINE
};
