/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var util = require('util');

var once = require('once');
var assert = require('assert-plus');

var common = require('./common');
var control = require('../control');
var dtrace = require('../dtrace');

var ObjectNotFoundError = require('../errors').ObjectNotFoundError;
var InternalError = require('../errors').InternalError;


// --- Globals

var ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'key', type: 'string' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    checkCache,
    function dbConnect(r, cb) {
        if (r.object) {
            cb(null);
        } else {
            control.getPGHandle(r, cb);
        }
    },
    function loadBucket(r, cb) {
        if (r.object) {
            cb();
        } else {
            common.loadBucket(r, cb);
        }
    },
    sendHandledOptions,
    loadObject
];

var HANDLED_GETOBJECT_OPTIONS = [
    'noBucketCache',
    'noCache',
    'req_id',
    'requireOnlineReindexing',
    'timeout'
];


// --- Handlers

function checkCache(req, cb) {
    req.cacheKey = common.cacheKey(req.bucket.name, req.key);

    if (req.opts.noCache) {
        cb();
        return;
    }

    req.object = req.cache.get(req.cacheKey);
    cb();
}


/**
 * Sends a record that contains the list of getObject request options that the
 * server handles. This allows Moray clients to compare this set of handled
 * options with what they expect the server to handle.
 */
function sendHandledOptions(req, cb) {
    var res = req.rpc;

    if (req.opts && req.opts.internalOpts &&
        req.opts.internalOpts.sendHandledOptions === true) {
        res.write({ _handledOptions: HANDLED_GETOBJECT_OPTIONS });
    }

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
            return;
        }

        var ignore = common.getReindexingFields(req.bucket);

        /*
         * Attempt to convert the database row.  This may fail if the "_id"
         * value is larger than can be represented as a Javascript number.
         */
        try {
            req.object = common.rowToObject(req.bucket, ignore, row);
        } catch (ex) {
            log.error({ err: ex, bucket: req.bucket, ignore: ignore, row: row },
              'loadObject: failed to convert row to object');
            cb(new InternalError(ex, 'failed to convert row: %s', ex.message));
            return;
        }

        req.cache.set(req.cacheKey, req.object);
        log.debug({
            object: req.object
        }, 'loadObject: done');
        cb();
    });
}



// --- Handlers

function get(options) {
    control.assertOptions(options);
    assert.object(options.objectCache);

    function _get(rpc) {
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
        req.cache = options.objectCache;
        req.key = key;

        dtrace['getobject-start'].fire(function () {
            return ([req.msgid, req.req_id, bucket, key]);
        });
        req.log.debug({
            bucket: bucket,
            key: key,
            opts: opts
        }, 'getObject: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: function () { return req.object; },
            cbProbe: function () {
                var val = JSON.stringify(req.object);
                return ([req.msgid, req.req_id, val]);
            },
            skipTransactionSql: true
        });
    }

    return (_get);
}



// --- Exports

module.exports = {
    get: get
};
