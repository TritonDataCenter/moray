/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var once = require('once');
var assert = require('assert-plus');

var common = require('./common');
var control = require('../control');
var dtrace = require('../dtrace');

var ObjectNotFoundError = require('../errors').ObjectNotFoundError;


///--- Handlers

function checkCache(req, cb) {
    req.cacheKey = common.cacheKey(req.bucket.name, req.key);

    if (req.opts.noCache) {
        cb();
        return;
    }

    req.object = req.cache.get(req.cacheKey);
    cb();
}

function loadObject(req, cb) {
    if (req.object) {
        cb();
        return;
    }

    var bucket = req.bucket.name;
    var log = req.log;
    var q;
    var row;
    var sql = util.format(('SELECT *, \'%s\' AS req_id ' +
                           'FROM %s WHERE _key=$1'),
                          req.req_id, bucket);

    cb = once(cb);

    log.debug({
        bucket: req.bucket.name,
        key: req.key
    }, 'loadObject: entered');

    q = req.pg.query(sql, [req.key]);

    q.once('error', function (err) {
        log.debug(err, 'loadObject: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        if (!row) {
            cb(new ObjectNotFoundError(bucket, req.key));
        } else {
            req.object = common.rowToObject(req.bucket, row);
            req.cache.set(req.cacheKey, req.object);
            log.debug({
                object: req.object
            }, 'loadObject: done');
            cb();
        }
    });
}



///--- Handlers

function get(options) {
    control.assertOptions(options);
    assert.object(options.objectCache);
    var route = 'getObject';

    function _get(bucket, key, opts, res) {
        var req = control.buildReq(opts, res, options);
        req.bucket = {
            name: bucket
        };
        req.cache = options.objectCache;
        req.key = key;

        dtrace['getobject-start'].fire(function () {
            return ([res.msgid, req.req_id, bucket, key]);
        });
        req.log.debug({
            bucket: bucket,
            key: key,
            opts: opts
        }, '%s: entered', route);
        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                checkCache,
                function dbConnect(r, cb) {
                    if (r.object) {
                        cb(null);
                    } else {
                        var fetch = control.getPGHandle(route, options.manatee,
                                {async: false, read: true});
                        fetch(r, cb);
                    }
                },
                function loadBucket(r, cb) {
                    if (r.object) {
                        cb();
                    } else {
                        common.loadBucket(r, cb);
                    }
                },
                loadObject
            ],
            cbOutput: function () { return req.object; },
            cbProbe: function () {
                var val = JSON.stringify(req.object);
                return ([req.msgid, val]);
            }
        });
    }

    return (_get);
}



///--- Exports

module.exports = {
    get: get
};
