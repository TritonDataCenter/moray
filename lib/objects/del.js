/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

// This is exported so that batch put can leverage it
var PIPELINE = [
    common.loadBucket,
    drop,
    common.runPostChain
];

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
    var route = 'delObject';

    function _del(bucket, key, opts, res) {
        var req = control.buildReq(opts, res, options.log);
        req.bucket = {
            name: bucket
        };
        req.key = key;
        req.etag = (opts.etag || opts._etag);

        dtrace['delobject-start'].fire(function () {
            return ([res.msgid, req.req_id, bucket, key]);
        });
        req.log.debug({
            bucket: bucket,
            key: key,
            opts: opts
        }, 'delObject: entered');

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee)
            ].concat(PIPELINE),
            cbOutput: common.stdOutput(req)
        });
    }

    return (_del);
}



///--- Exports

module.exports = {
    del: del,
    pipeline: PIPELINE
};
