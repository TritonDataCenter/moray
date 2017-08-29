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
var once = require('once');
var vasync = require('vasync');

var objCommon = require('../objects/common');

var mod_errors = require('../errors');
var InvalidBucketConfigError = mod_errors.InvalidBucketConfigError;
var InvalidBucketNameError = mod_errors.InvalidBucketNameError;
var NotFunctionError = mod_errors.NotFunctionError;

var typeToPg = require('../pg').typeToPg;

var mod_schema = require('../schema');

var TYPES = require('../types').TYPES;


///--- Globals

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
    '_key',
    '_atime',
    '_ctime',
    '_mtime',
    '_rver',
    '_txn_snap',
    '_value',
    '_vnode'
];


///--- Internal helpers

function indexName(bucket, field) {
    assert.string(bucket, 'bucket');
    assert.string(field, 'field');

    return bucket + '_' + field + '_idx';
}


///--- API

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
        var idxName = indexName(bucket, i.name);
        var sql = util.format(
            'CREATE %s INDEX %s ON %s USING %s (%s) WHERE %s IS NOT NULL',
            (opts.unique ? 'UNIQUE' : ''), idxName,
            bucket, i.type, i.name, i.name);
        return (sql);
    });

    log.debug({bucket: bucket}, 'createIndexes: entered');

    vasync.forEachParallel({
        inputs: queries,
        func: function createIndex(sql, cb) {
            log.debug('createIndexes: running %j', sql);

            var q = pg.query(sql);

            q.once('error', function (err) {
                log.error({
                    err: err,
                    sql: sql
                }, 'createIndex: failed');

                cb(err);
            });

            q.once('end', function (_) {
                cb();
            });
        }
    }, callback);
}


function dropIndexes(opts, callback) {
    var bucket = opts.bucket;
    var log = opts.log;

    var queries = opts.indexes.map(function (name) {
        return util.format('DROP INDEX IF EXISTS %s',
            indexName(bucket, name));
    });

    log.debug({bucket: bucket}, 'dropIndexes: entered');

    vasync.forEachParallel({
        inputs: queries,
        func: function dropIndex(sql, cb) {
            log.debug('dropIndexes: running %j', sql);
            var q = opts.pg.query(sql);

            q.once('error', function (err) {
                log.error({
                    err: err,
                    sql: sql
                }, 'dropIndex: failed');

                cb(err);
            });

            q.once('end', function (_) {
                cb();
            });
        }
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
        return (cb(err));
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
        return (cb(new NotFunctionError(e, 'trigger not function')));
    }

    if (!BUCKET_NAME_RE.test(bucket.name))
        return (cb(new InvalidBucketNameError(bucket.name)));

    if (RESERVED_BUCKETS.indexOf(bucket.name) !== -1)
        return (cb(new InvalidBucketNameError(bucket.name)));

    try {
        assert.arrayOfFunc(bucket.post);
    } catch (e) {
        log.debug(e, 'validation of post failed');
        return (cb(new NotFunctionError('post')));
    }

    try {
        assert.arrayOfFunc(bucket.pre);
    } catch (e) {
        log.debug(e, 'validation of pre failed');
        return (cb(new NotFunctionError('pre')));
    }

    err = validateIndexes(bucket.index);
    if (err !== null) {
        return (cb(err));
    }

    log.debug('validate: done');
    return (cb());
}


function shootdownBucket(req, cb) {
    objCommon.shootdownBucket(req);
    cb();
}



///--- Exports

module.exports = {
    INDEX_TYPES: INDEX_TYPES,
    BUCKET_NAME_RE: BUCKET_NAME_RE,
    RESERVED_BUCKETS: RESERVED_BUCKETS,
    buildIndexString: buildIndexString,
    createIndexes: createIndexes,
    dropIndexes: dropIndexes,
    mapIndexType: mapIndexType,
    shootdownBucket: shootdownBucket,
    validateBucket: validateBucket
};
