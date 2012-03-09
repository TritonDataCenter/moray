// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var restify = require('restify');

var args = require('./args');


///--- Globals

var assertArgument = args.assertArgument;
var sprintf = util.format;



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

function BucketAlreadyExistsError(bucket) {
    assertArgument('bucket', 'string', bucket);
    restify.RestError.call(this,
                           409,
                           'BucketAlreadyExists',
                           sprintf('/%s already exists', bucket),
                           BucketAlreadyExistsError);
    this.name = 'BucketAlreadyExistsError';
}
util.inherits(BucketAlreadyExistsError, restify.RestError);


function EtagConflictError(bucket, key, etag) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assertArgument('etag', 'string', etag);

    restify.PreconditionFailedError.call(this,
                                         '/%s/%s: etag conflict: %s',
                                         bucket, key, etag);
}
util.inherits(EtagConflictError, restify.PreconditionFailedError);


function IndexTypeError(bucket, index, type) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('index', 'string', index);
    assertArgument('type', 'string', type);

    restify.InvalidArgumentError.call(this,
                                      '/%s?index=%s is of type %s.',
                                      bucket, index, type);

}
util.inherits(IndexTypeError, restify.InvalidArgumentError);


function InvalidBucketNameError(bucket) {
    assertArgument('bucket', 'string', bucket);

    // Note it's not a big deal to hardcode this regex here; it's RFC3986's
    // unreserved charspace.
    restify.InvalidArgumentError.call(this,
                                      '%s is an invalid bucket name. Syntax ' +
                                      ' must be of the form /[a-zA-Z0-9_-.~]+/',
                                      bucket);

}
util.inherits(IndexTypeError, restify.InvalidArgumentError);


function NotIndexedError(bucket, index) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('index', 'string', index);

    restify.InvalidArgumentError.call(this,
                                      '/%s does not have an index %s',
                                      bucket,
                                      index);

}
util.inherits(NotIndexedError, restify.InvalidArgumentError);


/**
 * Extension over standard ResourceNotFoundError, where we standardize
 * the error messages via a URN scheme.
 *
 * @arg {string} bucket - the bucket name.
 * @arg {string} key    - optional key name.
 */
function ObjectNotFoundError(bucket, key) {
    assertArgument('bucket', 'string', bucket);
    restify.ResourceNotFoundError.call(this,
                                       '/%s/%s does not exist',
                                       bucket, key || '');
}
util.inherits(ObjectNotFoundError, restify.ResourceNotFoundError);


/**
 * Error to return when a key is not found, but it is in the tombstone.
 *
 * Note this is a 'RestError' as in restify, so it can be returned directly
 * back to clients.  HttpCode will be 410, and the message will conain a
 * moray urn and deletion time.
 *
 * @constructor
 * @arg {string} bucket - bucket name.
 * @arg {string} key    - key name.
 * @arg {string} dtime  - deletion time (from tombstone table).
 * @throws {TypeError} if bucket is not a string.
 */
function ResourceGoneError(bucket, key, dtime) {
    assertArgument('bucket', 'string', bucket);
    assertArgument('key', 'string', key);
    assert.ok('dtime');

    restify.RestError.call(this,
                           410,
                           'ResourceGone',
                           sprintf('/%s/%s was deleted at %s',
                                   bucket, key, ISODateString(dtime)),
                           ResourceGoneError);
    this.name = 'ResourceGoneError';
}
util.inherits(ResourceGoneError, restify.RestError);


///--- Exports

module.exports = {

    BucketAlreadyExistsError: BucketAlreadyExistsError,
    IndexTypeError: IndexTypeError,
    NotIndexedError: NotIndexedError,
    ObjectNotFoundError: ObjectNotFoundError,
    ResourceGoneError: ResourceGoneError

};

// Make it easy to access all Errors by injecting into the global
// namespace.
Object.keys(restify).forEach(function (k) {
    if (/\w+Error$/.test(k))
        global[k] = restify[k];
});

Object.keys(module.exports).forEach(function (k) {
    global[k] = module.exports[k];
});
