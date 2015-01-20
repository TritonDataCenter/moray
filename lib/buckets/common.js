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
var once = require('once');
var vasync = require('vasync');

require('../errors');



///--- Globals

var INDEX_TYPES = {
    string: true,
    number: true,
    'boolean': true,
    inet: true
};

// Postgres rules:
// start with a letter, everything else is alphum or '_', and must be
// <= 63 characters in length
var BUCKET_NAME_RE = /^[a-zA-Z]\w{0,62}$/;

var RESERVED_BUCKETS = ['moray', 'search'];


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


function mapIndexType(schema, name) {
    var i = {
        name: name
    };

    if (schema && schema[name] && schema[name].type &&
        /^\[\w+\]$/.test(schema[name].type)) {
        i.type = 'GIN';
    } else {
        i.type = 'BTREE';
    }

    return (i);
}


function createIndexes(opts, cb) {
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

    cb = once(cb);

    log.debug({bucket: bucket}, 'createIndexes: entered');
    vasync.forEachParallel({
        func: function createIndex(sql, _cb) {
            _cb = once(_cb);
            log.debug('createIndexes: running %s', sql);
            var q = pg.query(sql);
            q.once('error', function (err) {
                if (err) {
                    log.error({
                        err: err,
                        sql: sql
                    }, 'createIndex: failed (ignoring)');
                }
                _cb();
            });
            q.once('end', function () {
                _cb();
            });
        },
        inputs: queries
    }, cb);
}


function typeToPg(type) {
    assert.string(type, 'type');

    var pgType;

    switch (type) {
    case 'number':
        pgType = 'NUMERIC';
        break;
    case '[number]':
        pgType = 'NUMERIC[]';
        break;
    case 'boolean':
        pgType = 'BOOLEAN';
        break;
    case '[boolean]':
        pgType = 'BOOLEAN[]';
        break;
    case 'string':
        pgType = 'TEXT';
        break;
    case '[string]':
        pgType = 'TEXT[]';
        break;
    case 'inet':
        pgType = 'INET';
        break;
    case '[inet]':
        pgType = 'INET[]';
        break;
    default:
        throw new InvalidIndexDefinitionError(type);
    }

    return (pgType);
}


function validateIndexes(schema) {
    var i, j, k, k2, keys, msg, sub, subKeys;

    keys = Object.keys(schema);
    for (i = 0; i < keys.length; i++) {
        k = keys[i];
        if (typeof (k) !== 'string') {
            throw new InvalidBucketConfigError('keys must be ' +
                                               'strings');
        }
        if (typeof (schema[k]) !== 'object') {
            throw new InvalidBucketConfigError('values must be ' +
                                               'objects');
        }

        sub = schema[k];
        subKeys = Object.keys(sub);
        for (j = 0; j < subKeys.length; j++) {
            k2 = subKeys[j];
            switch (k2) {
            case 'type':
                if (sub[k2] !== 'string' &&
                    sub[k2] !== 'number' &&
                    sub[k2] !== 'boolean' &&
                    sub[k2] !== 'inet' &&
                    sub[k2] !== '[string]' &&
                    sub[k2] !== '[number]' &&
                    sub[k2] !== '[boolean]' &&
                    sub[k2] !== '[inet]') {
                    msg = k + '.type is invalid';
                    throw new InvalidBucketConfigError(msg);
                }
                break;
            case 'unique':
                if (typeof (sub[k2]) !== 'boolean') {
                    msg = k + '.unique must be boolean';
                    throw new InvalidBucketConfigError(msg);
                }
                break;
            default:
                msg = k + '.' + k2 + ' is invalid';
                throw new InvalidBucketConfigError(msg);
            }
        }
    }
}


function validateBucket(req, cb) {
    var bucket = req.bucket;
    var log = req.log;

    log.debug('validate: entered (%j)', bucket);

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

    if (typeof (bucket.index) !== 'object' ||
        Array.isArray(bucket.index)) {
        return (cb(new InvalidBucketConfigError('index is not ' +
                                                'an object')));
    }

    if (!Array.isArray(bucket.post))
        return (cb(new NotFunctionError('post')));

    try {
        assert.arrayOfFunc(bucket.post);
    } catch (e) {
        log.debug(e, 'validation of post failed');
        return (cb(new NotFunctionError('post')));
    }

    if (!Array.isArray(bucket.pre))
        return (cb(new NotFunctionError('pre')));

    try {
        assert.arrayOfFunc(bucket.pre);
    } catch (e) {
        log.debug(e, 'validation of pre failed');
        return (cb(new NotFunctionError('pre')));
    }

    try {
        validateIndexes(bucket.index);
    } catch (e) {
        return (cb(e));
    }

    if (typeof (bucket.options) !== 'object') {
        return (cb(new InvalidBucketConfigError('options is not ' +
                                                'an object')));
    }

    if (typeof (bucket.options.version) !== 'number') {
        return (cb(new InvalidBucketConfigError('options.version ' +
                                                'is not a number')));
    }

    log.debug('validate: done');
    return (cb());
}



///--- Exports

module.exports = {
    INDEX_TYPES: INDEX_TYPES,
    BUCKET_NAME_RE: BUCKET_NAME_RE,
    RESERVED_BUCKETS: RESERVED_BUCKETS,
    buildIndexString: buildIndexString,
    createIndexes: createIndexes,
    mapIndexType: mapIndexType,
    typeToPg: typeToPg,
    validateBucket: validateBucket
};
