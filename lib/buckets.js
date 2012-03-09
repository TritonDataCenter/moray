// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var crypto = require('crypto');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;



///--- Helpers

function paramToNumber(name, param, max) {
    assert.ok(name);
    assert.ok(param);

    try {
        var n = parseInt(param, 10);
        if (max)
            n = Math.min(n, max);

        return n;
    } catch (e) {
        throw new InvalidArgumentError('%s was not a valid number', name);
    }
}


function createMarker(req, opts) {
    assert.ok(req);
    assert.ok(req.config);
    assert.ok(req.config.marker);
    assert.ok(opts);

    var iv = req.config.marker.iv;
    var key = req.config.marker.key;
    var marker = '';

    var aes = crypto.createCipheriv('aes-128-cbc', key, iv);
    marker += aes.update(new Date().getTime() + '::', 'utf8', 'base64');
    marker += aes.update(JSON.stringify(opts), 'utf8', 'base64');
    marker += aes.final('base64');

    return marker;
}


function parseMarker(req, marker) {
    assert.ok(req);
    assert.ok(req.config);
    assert.ok(req.config.marker);
    assert.ok(marker);

    try {
        var dec = '';
        var iv = req.config.marker.iv;
        var key = req.config.marker.key;

        var aes = crypto.createDecipheriv('aes-128-cbc', key, iv);
        dec += aes.update(marker, 'base64', 'utf8');
        dec += aes.final('utf8');
        return JSON.parse(dec.split('::')[1]);
    } catch (e) {
        req.log.debug({err: e}, 'Error parsing marker');
        throw new InvalidArgumentError('marker is invalid');
    }
}



///--- Routes

function list(req, res, next) {
    assert.ok(req.db);

    var db = req.db;
    var log = req.log;

    log.debug('ListBuckets entered');
    return db.buckets(function (err, buckets) {
        if (err)
            return next(err);

        log.debug({buckets: buckets}, 'ListBuckets done');
        res.send(buckets);
        return next();
    });
}


function create(req, res, next) {
    assert.ok(req.db);

    var db = req.db;
    var i;
    var indexes = {};
    var log = req.log;

    function invalidIndexFormat() {
        return next(new InvalidArgumentError('index is an invalid format'));
    }

    if (!req.params.bucket)
        return next(new MissingParameterError('bucket is required'));

    if (typeof (req.params.bucket) !== 'string')
        return next(new InvalidArgumentError('bucket must be a string'));

    if (!/[a-zA-Z0-9_\-\.~]+/.test(req.params.bucket))
        return next(new InvalidBucketNameError(req.params.bucket));

    if (req.params.bucket === 'search')
        return next(new InvalidArgumentError(req.params.bucket +
                                             ' is a reserved name.'));

    if (req.params.index) {
        if (typeof (req.params.index) === 'string') {
            indexes[req.params.index] = {
                type: 'string',
                unique: false
            };
        } else if (Array.isArray(req.params.index)) {
            for (i = 0; i < req.params.index.length; i++) {
                if (typeof (req.params.index[i]) !== 'string')
                    return invalidIndexFormat();

                indexes[req.params.index[i]] = {
                    type: 'string',
                    unique: false
                };
            }
        } else if (typeof (req.params.index) === 'object') {
            var indexKeys = Object.keys(req.params.index);
            for (i = 0; i < indexKeys.length; i++) {
                if (typeof (req.params.index[indexKeys[i]]) !== 'object' ||
                    typeof (req.params.index[indexKeys[i]].type) !== 'string') {

                    return invalidIndexFormat();
                }
            }
            indexes = req.params.index;
        } else {
            return invalidIndexFormat();
        }
    }

    log.debug({
        bucket: req.params.bucket,
        indexes: indexes
    }, 'CreateBucket entered');

    return db.createBucket(req.params.bucket, indexes, function (err, bucket) {
        if (err)
            return next(err);

        log.debug({bucket: bucket}, 'CreateBucket done');
        res.header('Location', '/%s', req.params.bucket);
        res.send(201, bucket);
        return next();
    });
}


function getBucket(req, res, next) {
    assert.ok(req.db);

    var db = req.db;
    var log = req.log;

    log.debug({bucket: req.params.bucket}, 'GetBucket entered');

    req.bucket = {};
    if (!req.params.schema)
        return next();

    return db.buckets(function (err, buckets) {
        if (err)
            return next(err);

        if (!buckets[req.params.bucket])
            return next(new ObjectNotFoundError(req.params.bucket));

        req.bucket = buckets[req.params.bucket];
        return next();
    });
}


function listKeys(req, res, next) {
    assert.ok(req.db);

    var opts;

    try {
        if (req.params.marker) {
            opts = parseMarker(req, req.params.marker);
        } else {
            opts = {
                limit: 1000,
                offset: 0
            };
            if (req.params.limit)
                opts.limit = paramToNumber('limit', req.params.limit, 1000);
            if (req.params.prefix)
                opts.prefix = req.params.prefix;
        }
    } catch (e) {
        return next(e);
    }

    return req.db.list(req.params.bucket, opts, function (err, keys) {
        if (err)
            return next(err);

        req.bucket.keys = {};
        res.header('x-total-keys', keys.total);

        keys.keys.forEach(function (k) {
            req.bucket.keys[k.key] = {
                etag: k.etag,
                mtime: k.mtime
            };
        });

        if ((keys.keys.length + opts.offset) < keys.total) {
            opts.offset = opts.offset + opts.limit;
            var marker = createMarker(req, opts);
            res.link(sprintf('/%s?marker=%s',
                             req.params.bucket,
                             encodeURIComponent(marker)),
                     'next');
        }

        return next();
    });
}


function getBucketDone(req, res, next) {
    req.log.debug({
        bucket: req.params.bucket,
        body: req.bucket
    }, 'GetBucket done');
    res.send(req.bucket);
    return next();
}


function del(req, res, next) {
    assert.ok(req.db);

    var db = req.db;
    var log = req.log;

    log.debug({bucket: req.params.bucket}, 'DeleteBucket entered');
    return db.buckets(function (err, buckets) {
        if (err)
            return next(err);

        if (!buckets[req.params.bucket])
            return next(new ObjectNotFoundError(req.params.bucket));

        return db.deleteBucket(req.params.bucket, function (err2) {
            if (err2)
                return next(err2);

            log.debug({bucket: req.params.bucket}, 'DeleteBucket done');
            res.send(204);
            return next();
        });
    });
}


function search(req, res, next) {
    assert.ok(req.db);

    var attr;
    var db = req.db;
    var log = req.log;
    var val;

    // TODO: Support AND/OR queries
    Object.keys(req.params).forEach(function (k) {
        if (k === 'bucket')
            return;

        attr = k;
        val = req.params[k];
    });

    if (!attr || !val)
        return next(new MissingParameterError('An attr/val is required'));

    log.debug({
        bucket: req.params.bucket,
        attribute: attr,
        value: val
    }, 'SearchBucket entered');
    return db.find(req.params.bucket, attr, val, function (err, query) {
        if (err)
            return next(err);

        var done = false;
        var objects = [];

        query.on('error', function (err) {
            if (done)
                return;

            done = true;
            return next(err);
        });

        query.on('entry', function (obj) {
            objects.push(obj.data);
        });

        query.on('end', function() {
            if (done)
                return;

            done = true;
            var md5 = crypto.createHash('md5');
            md5.update(JSON.stringify(objects), 'utf8');
            res.etag = md5.digest('hex');
            res.send(objects);
            return next();
        });
    });
}



///--- Exports

module.exports = {
    mount: function mount(server) {
        args.assertArgument(server, 'object', server);

        server.get({path: '/', name: 'ListBuckets'}, list);
        server.head({path: '/', name: 'ListBuckets'}, list);

        var bodyParser = restify.bodyParser();
        server.post({path: '/', name: 'CreateBucket'}, bodyParser, create);

        server.get({path: '/:bucket', name: 'GetBucket'},
                   getBucket, listKeys, getBucketDone);
        server.head({path: '/:bucket', name: 'GetBucket'},
                    getBucket, listKeys, getBucketDone);

        server.del({path: '/:bucket', name: 'DeleteBucket'}, del);

        server.get({path: '/search/:bucket', name: 'SearchBucket'}, search);
        return server;
    }
};
