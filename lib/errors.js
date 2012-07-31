// Copyright 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var verror = require('verror');




///--- Globals

var WError = verror.WError;

var slice = Function.prototype.call.bind(Array.prototype.slice);



///--- Helpers

function ISODateString(d) {
    function pad(n) {
        return n < 10 ? '0' + n : n;
    }

    if (typeof (d) === 'string')
        d = new Date(d);

    return d.getUTCFullYear() + '-'
        + pad(d.getUTCMonth()+1) + '-'
        + pad(d.getUTCDate()) + 'T'
        + pad(d.getUTCHours()) + ':'
        + pad(d.getUTCMinutes()) + ':'
        + pad(d.getUTCSeconds()) + 'Z';
}



///--- Errors

function BucketNotFoundError(cause, bucket) {
        if (arguments.length === 1) {
                bucket = cause;
                cause = {};
        }
        assert.string(bucket, 'bucket');

        WError.call(this, cause, '%s does not exist', bucket);
}
util.inherits(BucketNotFoundError, WError);


function InternalError() {
        WError.apply(this, arguments);
}
util.inherits(InternalError, WError);


function InvalidBucketNameError(cause, bucket) {
        if (arguments.length === 1) {
                bucket = cause;
                cause = {};
        }
        assert.string(bucket, 'bucket');

        WError.call(this, cause, '%s is not a valid bucket name', bucket);
}
util.inherits(InvalidBucketNameError, WError);


function InvalidIndexError() {
        WError.apply(this, arguments);
}
util.inherits(InvalidIndexError, WError);


function InvalidIndexDefinitionError(cause, type) {
        if (arguments.length === 1) {
                type = cause;
                cause = {};
        }
        assert.string(type, 'type');

        WError.call(this, cause,
                    '%s is an invalid index type. Supported types are %j',
                    ['boolean', 'number', 'string'], type);
}
util.inherits(InvalidIndexTypeError, WError);


function InvalidIndexTypeError(cause, name, type) {
        if (arguments.length === 2) {
                type = name;
                name = cause;
                cause = {};
        }
        assert.string(name, 'name');
        assert.string(type, 'type');

        WError.call(this, cause, 'index(%s) is of type %s', name, type);
}
util.inherits(InvalidIndexTypeError, WError);


function InvalidQueryError(cause, filter) {
        if (arguments.length === 1) {
                filter = cause;
                cause = {};
        }
        assert.string(filter, 'filter');
        WError.call(this, cause, '%s is an invalid filter', filter);
}
util.inherits(InvalidQueryError, WError);


function NotFunctionError(cause, name) {
        if (arguments.length === 1) {
                name = cause;
                cause = {};
        }
        assert.string(name, 'name');

        WError.call(this, cause, '%s must be [Function]', name);
}
util.inherits(NotFunctionError, WError);


function NotIndexedError(cause, bucket, filter) {
        if (arguments.length === 2) {
                filter = bucket;
                bucket = cause;
                cause = {};
        }
        assert.string(bucket, 'bucket');
        assert.string(filter, 'filter');

        WError.call(this, cause,
                    '%s does not have any indexes that support %s',
                    bucket, filter);
}
util.inherits(NotIndexedError, WError);


function ObjectNotFoundError(cause, bucket, key) {
        if (arguments.length === 2) {
                key = bucket;
                bucket = cause;
                cause = {};
        }
        assert.string(bucket, 'bucket');
        assert.string(key, 'key');

        WError.call(this, cause, '%s::%s does not exist', bucket, key);
}
util.inherits(ObjectNotFoundError, WError);


function SchemaChangeError(cause, bucket, attribute) {
        if (arguments.length === 2) {
                attribute = bucket;
                bucket = cause;
                cause = {};
        }
        assert.string(bucket, 'bucket');
        assert.string(attribute, 'attribute');

        WError.call(this, cause, '%s: an index already exists for %s',
                    bucket, attribute);
}
util.inherits(SchemaChangeError, WError);


function UniqueAttributeError(cause, attribute, value) {
        if (arguments.length === 2) {
                value = attribute;
                attribute = cause;
                cause = {};
        }
        assert.string(attribute, 'attribute');
        assert.string(value, 'value');
        WError.call(this,
                    cause || {},
                    '"%s" is a unique attribute; value "%s" already exists',
                    attribute,
                    value);
}
util.inherits(UniqueAttributeError, WError);



///--- Exports

// Auto export all Errors defined in this file
fs.readFileSync(__filename, 'utf8').split('\n').forEach(function (l) {
        /* JSSTYLED */
        var match = /^function\s+(\w+)\(.*/.exec(l);
        if (match !== null && Array.isArray(match) && match.length > 1) {
                if (/\w+Error$/.test(match[1])) {
                        module.exports[match[1]] = eval(match[1]);
                }
        }
});


Object.keys(module.exports).forEach(function (k) {
        global[k] = module.exports[k];
});
