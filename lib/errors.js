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


function InvalidIndexTypeError(cause, type) {
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


function InvalidQueryError(cause, filter) {
        if (arguments.length === 1) {
                filter = cause;
                cause = {};
        }
        assert.string(filter, 'filter');
        WError.call(this, cause, '%s is an invalid filter', filter);
}
util.inherits(InvalidQueryError, WError);


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


function NotFunctionError(cause, name) {
        if (arguments.length === 1) {
                name = cause;
                cause = {};
        }
        assert.string(name, 'name');

        WError.call(this, cause, '%s must be [Function]', name);
}
util.inherits(NotFunctionError, WError);


function SchemaChangeError(bucket, attribute) {
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



///--- Errors

// function BucketAlreadyExistsError(bucket) {
//     assertString('bucket', bucket);

//     restify.RestError.call(this,
//                            409,
//                            'BucketAlreadyExists',
//                            sprintf('/%s already exists', bucket),
//                            BucketAlreadyExistsError);

//     this.name = 'BucketAlreadyExistsError';
// }
// util.inherits(BucketAlreadyExistsError, restify.RestError);


// function BucketNotFoundError(bucket) {
//     assertString('bucket', bucket);

//     restify.ResourceNotFoundError.call(this, '/%s is not a known bucket',
//                                        bucket);

//     this.name = 'BucketNotFoundError';
// }
// util.inherits(BucketNotFoundError, restify.ResourceNotFoundError);


// function EtagConflictError(bucket, key, etag) {
//     assertString('bucket', bucket);
//     assertString('key', key);
//     assertString('etag', etag);

//     restify.PreconditionFailedError.call(this,
//                                          '/%s/%s: etag conflict: %s',
//                                          bucket, key, etag);

//     this.name = 'EtagConflictError';
// }
// util.inherits(EtagConflictError, restify.PreconditionFailedError);


// function IndexTypeError(bucket, index, type) {
//     assertString('bucket', bucket);
//     assertString('index', index);
//     assertString('type', type);

//     restify.InvalidArgumentError.call(this,
//                                       '/%s?index=%s is of type %s',
//                                       bucket, index, type);

//     this.name = 'IndexTypeError';

// }
// util.inherits(IndexTypeError, restify.InvalidArgumentError);


// function InvalidBucketNameError(bucket) {
//     assertString('bucket', bucket);

//     restify.InvalidArgumentError.call(this,
//                                       '%s is an invalid bucket name. Syntax ' +
//                                       'must be of the form /^[a-zA-Z]\w+$/ ' +
//                                       'and <= 63 characters',
//                                       bucket);

//     this.name = 'InvalidBucketNameError';

// }
// util.inherits(InvalidBucketNameError, restify.InvalidArgumentError);


// function InvalidIndexTypeError(type) {
//     assertString('type', type);

//     restify.InvalidArgumentError.call(this,
//                                       '%s is an invalid index type. ' +
//                                       'Supported types are %s',
//                                       [
//                                           'boolean',
//                                           'number',
//                                           'string'
//                                       ].join(', '),
//                                       type);
//     this.name = 'InvalidIndexTypeError';
// }
// util.inherits(InvalidIndexTypeError, restify.InvalidArgumentError);


// function InvalidSchemaError(msg) {
//     restify.InvalidArgumentError.call(this, 'Invalid schema: %s', msg || '');
//     this.name = 'InvalidSchemaError';
// }
// util.inherits(InvalidSchemaError, restify.InvalidArgumentError);



// function NotIndexedError(bucket, filter) {
//     assertString('bucket', bucket);
//     assertString('filter', filter);

//     restify.InvalidArgumentError.call(this,
//                                       '/%s does not have any indexes that ' +
//                                       'support %s',
//                                       bucket,
//                                       filter);

//     this.name = 'NotIndexedError';
// }
// util.inherits(NotIndexedError, restify.InvalidArgumentError);


// function ObjectNotFoundError(bucket, key) {
//     assertString('bucket', bucket);
//     assertString('key', key);

//     restify.ResourceNotFoundError.call(this,
//                                        '/%s%s does not exist',
//                                        bucket, key);

//     this.name = 'ObjectNotFoundError';
// }
// util.inherits(ObjectNotFoundError, restify.ResourceNotFoundError);


// function ReservedBucketError(bucket) {
//     assertString('bucket', bucket);

//     restify.InvalidArgumentError.call(this,
//                                       '%s is a reserved bucket name',
//                                       bucket);
//     this.name = 'ReservedBucketError';
// }
// util.inherits(ReservedBucketError, restify.InvalidArgumentError);


// function ResourceGoneError(bucket, key, dtime) {
//     assertString('bucket', bucket);
//     assertString('key', key);
//     assert.ok(dtime instanceof Date);

//     restify.RestError.call(this,
//                            410,
//                            'ResourceGone',
//                            sprintf('/%s/%s was deleted at %s',
//                                    bucket, key, ISODateString(dtime)),
//                            ResourceGoneError);

//     this.name = 'ResourceGoneError';
// }
// util.inherits(ResourceGoneError, restify.RestError);


// function SchemaChangeError(bucket, attribute) {
//     assertString('bucket', bucket);
//     assertString('attribute', attribute);

//     restify.InvalidArgumentError.call(this,
//                                       '/%s: an index already exists for %s; ' +
//                                       ' you cannot change types or uniqueness',
//                                       bucket, attribute);
//     this.name = 'SchemaChangeError';
// }
// util.inherits(SchemaChangeError, restify.InvalidArgumentError);


// function TombstoneNotFoundError(bucket, key) {
//     assertString('bucket', bucket);
//     assertString('key', key);

//     restify.ResourceNotFoundError.call(this,
//                                        '/%s%s/tombstone does not exist',
//                                        bucket, key);
//     this.name = 'TombstoneNotFoundError';
// }
// util.inherits(TombstoneNotFoundError, restify.ResourceNotFoundError);


// function UniqueAttributeError(index, value) {
//     assertString('index', index);
//     assertString('value', value);

//     restify.InvalidArgumentError.call(this,
//                                       '%s is a unique attribute and value ' +
//                                       '\'%s\' already exists',
//                                       index, value);

//     this.name = 'UniqueAttributeError';
// }
// util.inherits(UniqueAttributeError, restify.InvalidArgumentError);



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
