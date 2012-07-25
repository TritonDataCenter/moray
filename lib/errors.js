// Copyright 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var restify = require('restify');




///--- Globals

var RestError = restify.RestError;

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

function BucketNotFoundError(bucket) {
    assert.string(bucket, 'bucket');

        var opts = {
                constructorOpt: BucketNotFoundError,
                restCode: 'BucketNotFound',
                statusCode: 404,

        };
        RestError.call(this, opts, '%s does not exist', bucket);
}
util.inherits(BucketNotFoundError, RestError);


function InvalidIndexTypeError(name, type) {
        assert.string(name, 'name');
        assert.string(type, 'type');

        var opts = {
                constructorOpt: IndexIndexTypeError,
                restCode: 'InvalidIndexType',
                statusCode: 400
        };
        RestError.call(this, opts, 'index(%s) is of type %s', name, type);
}
util.inherits(InvalidIndexTypeError, RestError);


function InvalidBucketNameError(bucket) {
        assert.string(bucket, 'bucket');

        var opts = {
                constructorOpt: InvalidBucketNameError,
                restCode: 'InvalidBucketName',
                statusCode: 400,

        };
        RestError.call(this, opts, '%s is not a valid bucket name', bucket);
}
util.inherits(InvalidBucketNameError, RestError);


function InvalidIndexError(msg) {
        assert.string(msg, 'message');

        var opts = {
                constructorOpt: InvalidIndexError,
                restCode: 'InvalidIndexDefinition',
                statusCode: 400,
                message: msg

        };
        RestError.call(this, opts);
}
util.inherits(InvalidIndexError, RestError);


function InvalidIndexTypeError(type) {
    assert.string(type, 'type');

        var opts = {
                constructorOpt: InvalidIndexTypeError,
                restCode: 'InvalidIndexType',
                statusCode: 400
        };
        RestError.call(this,
                       opts,
                       '%s is an invalid index type. ' +
                       'Supported types are %s',
                       ['boolean', 'number', 'string'].join(', '),
                       type);
}
util.inherits(InvalidIndexTypeError, RestError);


function InvalidQueryError(filter) {
        var opts = {
                constructorOpt: InvalidQueryError,
                restCode: 'InvalidQuery',
                statusCode: 400
        };
        RestError.call(this,
                       opts,
                       '%s is an invalid filter',
                       filter);
}
util.inherits(InvalidQueryError, RestError);


function NotIndexedError(bucket, filter) {
        var opts = {
                constructorOpt: ObjectNotFoundError,
                restCode: 'NotIndexedError',
                statusCode: 400
        };
        RestError.call(this,
                       opts,
                       '/%s does not have any indexes that support %s',
                       bucket,
                       filter);
}
util.inherits(NotIndexedError, RestError);


function ObjectNotFoundError(bucket, key) {
        var opts = {
                constructorOpt: ObjectNotFoundError,
                restCode: 'ObjectNotFound',
                statusCode: 404
        };
        RestError.call(this,
                       opts,
                       '/%s/%s does not exist',
                       bucket,
                       key);
}
util.inherits(ObjectNotFoundError, RestError);


function NotFunctionError(name) {
        assert.string(name, 'name');

        var opts = {
                constructorOpt: NotFunctionError,
                restCode: 'InvalidArgument',
                statusCode: 400

        };
        RestError.call(this, opts, '%s must be [Function]', name);
}
util.inherits(NotFunctionError, RestError);


function SchemaChangeError(bucket, attribute) {
        assert.string(bucket, 'bucket');
        assert.string(attribute, 'attribute');

        var opts = {
                constructorOpt: SchemaChangeError,
                restCode: 'SchemaChange',
                statusCode: 400

        };
        RestError.call(this,
                       opts,
                       '/%s: an index already exists for %s; ' +
                       ' you cannot change types or uniqueness',
                       bucket, attribute);
}
util.inherits(SchemaChangeError, RestError);



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


// Make it easy to access all Errors by injecting into the global
// namespace (both restify and $self errors).
Object.keys(restify).forEach(function (k) {
        if (/\w+Error$/.test(k))
                global[k] = restify[k];
});

Object.keys(module.exports).forEach(function (k) {
        global[k] = module.exports[k];
});
