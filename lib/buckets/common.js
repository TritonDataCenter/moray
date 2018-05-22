/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var vasync = require('vasync');

var objCommon = require('../objects/common');

var mod_errors = require('../errors');
var InvalidBucketConfigError = mod_errors.InvalidBucketConfigError;
var InvalidBucketNameError = mod_errors.InvalidBucketNameError;
var NotFunctionError = mod_errors.NotFunctionError;

var typeToPg = require('../pg').typeToPg;

var mod_schema = require('../schema');

var TYPES = require('../types').TYPES;


// --- Globals

var INDEX_TYPES = {
    string: true,
    number: true,
    'boolean': true,
    ip: true,
    subnet: true
};

// Postgres rules:
// start with a letter, everything else is alphum or '_', and must be
// <= 63 characters in length
var BUCKET_NAME_RE = /^[a-zA-Z]\w{0,62}$/;

var INDEX_NAME_RE =
    /^(_[a-zA-Z0-9]+|[a-zA-Z][_a-zA-Z0-9]*[a-zA-Z0-9]|[a-zA-Z])$/;

var RESERVED_BUCKETS = [
    'buckets_config',
    'moray',
    'search'
];

var RESERVED_INDEXES = [
    '_etag',
    '_id',
    '_idx',
    '_key',
    '_atime',
    '_ctime',
    '_mtime',
    '_rver',
    '_txn_snap',
    '_value',
    '_vnode'
];


// --- API

function buildIndexString(schema) {
    assert.object(schema, 'schema');

    var str = '';
    Object.keys(schema).forEach(function (k) {
        str += ',\n        ' + k + ' ' + typeToPg(schema[k].type);
        if (schema[k].unique)
            str += ' UNIQUE';
    });

    return (str);
}


/**
 * Map a column name onto the kind of index that should be used with it. All
 * Moray-internal columns use BTREE, but user-specified columns use whatever
 * kind is appropriate for the type (e.g., array types usually use GIN).
 */
function mapIndexType(schema, name) {
    assert.object(schema, 'schema');
    assert.string(name, 'name');

    var type = 'BTREE';

    if (schema.hasOwnProperty(name)) {
        assert.object(TYPES[schema[name].type], 'valid type');
        type = TYPES[schema[name].type].index;
    }

    return {
        name: name,
        type: type
    };
}


function createIndexes(opts, callback) {
    var bucket = opts.bucket;
    var log = opts.log;
    var pg = opts.pg;

    var queries = opts.indexes.map(function (i) {
        var sql = util.format(('CREATE %s INDEX %s_%s_idx ' +
                               'ON %s USING %s (%s) ' +
                               'WHERE %s IS NOT NULL'),
                              (opts.unique ? 'UNIQUE' : ''), bucket, i.name,
                              bucket, i.type, i.name,
                              i.name);
        return (sql);
    });

    log.debug({bucket: bucket}, 'createIndexes: entered');
    vasync.forEachPipeline({
        func: function createIndex(sql, cb) {
            log.debug('createIndexes: running %s', sql);
            var q = pg.query(sql);
            q.once('error', function (err) {
                if (err) {
                    log.error({
                        err: err,
                        sql: sql
                    }, 'createIndex: failed');
                }
                cb(err);
            });
            q.once('end', function (_) {
                cb(null);
            });
        },
        inputs: queries
    }, callback);
}


function validateIndexes(schema) {
    var i, k, keys;

    keys = Object.keys(schema);
    for (i = 0; i < keys.length; i++) {
        k = keys[i];

        if (!INDEX_NAME_RE.test(k)) {
            return new InvalidBucketConfigError(
                'invalid index name "' + k + '"');
        }

        var kLC = k.toLowerCase();

        if (RESERVED_INDEXES.indexOf(kLC) !== -1) {
            return new InvalidBucketConfigError(
                'index name "' + k + '" is reserved for use by Moray');
        }

        if (/^_*moray/.test(kLC)) {
            return new InvalidBucketConfigError(
                'index names cannot start with "moray": "' + k + '"');
        }
    }

    return null;
}


function validateBucket(req, cb) {
    var bucket = req.bucket;
    var log = req.log;

    log.debug('validate: entered (%j)', bucket);

    var err = mod_schema.validateBucket(bucket);
    if (err !== null) {
        cb(err);
        return;
    }

    if (bucket.hasOwnProperty('options') &&
      bucket.options.hasOwnProperty('trackModification')) {
        cb(new InvalidBucketConfigError('"trackModification" is no longer ' +
          'supported'));
        return;
    }

    bucket.index = bucket.index || {};
    bucket.post = bucket.post || [];
    bucket.pre = bucket.pre || [];

    try {
        bucket.post = bucket.post.map(function (p) {
            var fn;
            fn = eval('fn = ' + p);
            return (fn);
        });
        bucket.pre = bucket.pre.map(function (p) {
            var fn;
            fn = eval('fn = ' + p);
            return (fn);
        });
    } catch (e) {
        log.debug(e, 'Invalid trigger function(s)');
        cb(new NotFunctionError(e, 'trigger not function'));
        return;
    }

    if (!BUCKET_NAME_RE.test(bucket.name)) {
        cb(new InvalidBucketNameError(bucket.name));
        return;
    }

    if (RESERVED_BUCKETS.indexOf(bucket.name) !== -1) {
        cb(new InvalidBucketNameError(bucket.name));
        return;
    }

    try {
        assert.arrayOfFunc(bucket.post);
    } catch (e) {
        log.debug(e, 'validation of post failed');
        cb(new NotFunctionError('post'));
        return;
    }

    try {
        assert.arrayOfFunc(bucket.pre);
    } catch (e) {
        log.debug(e, 'validation of pre failed');
        cb(new NotFunctionError('pre'));
        return;
    }

    err = validateIndexes(bucket.index);
    if (err !== null) {
        cb(err);
        return;
    }

    log.debug('validate: done');
    cb();
}


function shootdownBucket(req, cb) {
    objCommon.shootdownBucket(req);
    cb();
}



// --- Exports

module.exports = {
    INDEX_TYPES: INDEX_TYPES,
    BUCKET_NAME_RE: BUCKET_NAME_RE,
    RESERVED_BUCKETS: RESERVED_BUCKETS,
    buildIndexString: buildIndexString,
    createIndexes: createIndexes,
    mapIndexType: mapIndexType,
    shootdownBucket: shootdownBucket,
    validateBucket: validateBucket
};
