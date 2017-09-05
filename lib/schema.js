/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * This file contains schemas used for validating arguments to Moray
 * endpoints, such as base types, types of provided options, bucket
 * configurations, etc.
 *
 * These schemas are defined using JSON schema version 4.
 */

var Ajv = require('ajv');
var assert = require('assert-plus');

var mod_errors = require('./errors');
var InvocationError = mod_errors.InvocationError;
var InvalidBucketConfigError = mod_errors.InvalidBucketConfigError;

///--- Globals

var AJV_ENV = new Ajv();
var MORAY_TYPES = [
    'string', '[string]',
    'number', '[number]',
    'boolean', '[boolean]',
    'ip', '[ip]',
    'subnet', '[subnet]'
];


///--- Internal helpers

function allowedValuesText(allowed) {
    assert.array(allowed, 'allowed');

    var seen = {};
    var values = [];

    allowed.map(JSON.stringify).forEach(function (str) {
        var key = str.toUpperCase();
        if (!seen.hasOwnProperty(key)) {
            seen[key] = true;
            values.push(str);
        }
    });

    return ' (' + values.join(', ') + ')';
}


function errorMessage(err, name) {
    var msg = name + err.dataPath + ' ' + err.message;

    if (err.params.hasOwnProperty('allowedValues')) {
        msg += allowedValuesText(err.params.allowedValues);
    }

    return msg;
}


function errorsText(errs, name) {
    return errs.map(function (err) {
        return errorMessage(err, name);
    }).join(', ');
}


///--- Schema Declarations

AJV_ENV.addSchema({
    id: 'integer',
    type: 'integer',
    minimum: 0
});

AJV_ENV.addSchema({
    id: 'string',
    type: 'string',
    minLength: 1
});

AJV_ENV.addSchema({
    id: 'array',
    type: 'array'
});

AJV_ENV.addSchema({
    id: 'object',
    type: 'object',
    properties: {
        'hasOwnProperty': {
            description: '"hasOwnProperty" should not be on received objects',
            not: {
                type: [
                    'number',
                    'string',
                    'boolean',
                    'array',
                    'object',
                    'null'
                ]
            }
        }
    }
});

AJV_ENV.addSchema({
    id: 'sortobj',
    type: 'object',
    allOf: [ { '$ref': 'object' } ],
    properties: {
        attribute: {
            description: 'Column to sort on',
            type: 'string'
        },
        order: {
            description: 'Direction in which to sort',
            type: 'string',
            enum: [ 'ASC', 'asc', 'DESC', 'desc' ]
        }
    }
});

AJV_ENV.addSchema({
    id: 'options',
    type: 'object',
    allOf: [ { '$ref': 'object' } ],
    properties: {
        // Common options
        'timeout': {
            description: 'How long to wait for a response to SQL queries',
            type: 'integer',
            minimum: 0
        },
        'req_id': {
            description: 'A unique identifier for this request',
            type: 'string',
            format: 'uuid',
            maxLength: 36
        },

        // Object RPC options
        'etag': {
            description: 'The expected, current etag of the targeted object',
            type: [ 'null', 'string' ]
        },
        '_value': {
            description: 'An stringified version of the sent object',
            type: 'string'
        },
        'noBucketCache': {
            description: 'Whether to check the bucket cache for bucket schemas',
            type: 'boolean'
        },
        'noCache': {
            description: 'Whether to check the object cache before Postgres',
            type: 'boolean'
        },
        'headers': {
            description: 'Information to pass to pre/post-triggers',
            allOf: [ { '$ref': 'object' } ]
        },

        // Filter options
        'noLimit': {
            description: 'Skip using the default result limit',
            type: 'boolean'
        },
        'limit': {
            description: 'The maximum number of objects to return for a query',
            type: [ 'integer', 'string' ],
            pattern: '^[0-9]+$',
            minimum: 0
        },
        'offset': {
            description: 'An offset into a query result set',
            type: 'number',
            minimum: 0
        },
        'sort': {
            description: 'Sort the results of a query',
            anyOf: [
                { type: 'array', items: { '$ref': 'sortobj' } },
                { '$ref': 'sortobj' }
            ]
        },
        'no_count': {
            description: 'Skip calculating how many rows match the query',
            type: 'boolean'
        },
        'sql_only': {
            description: 'Return the SQL that would be executed ' +
                'instead of running it',
            type: 'boolean'
        },

        // Misc options
        'no_reindex': {
            type: 'boolean'
        },
        'deep': {
            type: 'boolean'
        }
    }
});

AJV_ENV.addSchema({
    id: 'requestobj',
    type: 'object',
    allOf: [ { '$ref': 'object' } ],
    required: [ 'bucket' ],
    properties: {
        operation: {
            description: 'The operation this request will perform',
            type: 'string',
            enum: [ 'put', 'delete', 'update', 'deleteMany' ]
        },
        bucket: {
            description: 'The bucket this request should operate on',
            type: 'string'
        },
        key: {
            description: 'The key in the bucket to affect',
            type: 'string'
        },
        filter: {
            description: 'A query describing which objects to affect',
            type: 'string'
        },
        fields: {
            description: 'The fields to update and their new values',
            allOf: [ { '$ref': 'object' } ]
        },
        value: {
            description: 'The value being inserted into the bucket',
            allOf: [ { '$ref': 'object' } ]
        },
        options: { '$ref': 'options' }
    }
});


AJV_ENV.addSchema({
    id: 'requests',
    type: 'array',
    items: { '$ref': 'requestobj' }
});


AJV_ENV.addSchema({
    id: 'index',
    type: 'object',
    allOf: [ { '$ref': 'object' } ],
    required: [ 'type' ],
    additionalProperties: false,
    properties: {
        'type': {
            type: 'string',
            enum: MORAY_TYPES
        },
        'unique': {
            type: 'boolean'
        }
    }
});

AJV_ENV.addSchema({
    id: 'bucket',
    type: 'object',
    allOf: [ { '$ref': 'object' } ],
    properties: {
        'index': {
            allOf: [ { '$ref': 'object' } ],
            patternProperties: {
                '.*': { '$ref': 'index' }
            }
        },
        'pre': {
            type: 'array'
        },
        'post': {
            type: 'array'
        },
        'options': {
            allOf: [ { '$ref': 'object' } ],
            properties: {
                'version': { '$ref': 'integer' },
                'trackModification': {
                    type: 'boolean'
                },
                'guaranteeOrder': {
                    type: 'boolean'
                },
                'syncUpdates': {
                    type: 'boolean'
                }
            }
        }
    }
});


///--- Exported functions

exports.validateArgument = function validateArgument(name, schema, value) {
    assert.string(name, 'name');
    assert.string(schema, 'schema name');

    if (AJV_ENV.validate(schema, value)) {
        return null;
    }

    return new InvocationError('%s', errorsText(AJV_ENV.errors, name));
};

exports.validateBucket = function validateBucket(bucket) {
    if (AJV_ENV.validate('bucket', bucket)) {
        return null;
    }

    return new InvalidBucketConfigError('%s',
        errorsText(AJV_ENV.errors, 'bucket'));
};
