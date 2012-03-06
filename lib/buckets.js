// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var crypto = require('crypto');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;
var InvalidArgumentError = restify.InvalidArgumentError;



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
        req.log.debug({err: e}, 'Error parsing %s -> %s', name, param);
        throw new InvalidArgumentError('%s was not a valid number', name);
    }
}


function createMarker(req, offset) {
    assert.ok(req);
    assert.ok(req.config);
    asssert.ok(req.config.marker);
    assert.ok(offset);

    var aes;
    var iv = req.config.marker.iv;
    var key = req.config.marker.key;

    aes = crypto.createCipheriv('aes128', key, iv);
    aes.update(new Date().getTime() + '::' + offset, 'utf8');

    return encodeURIComponent(aes.final('base64'));
}


function parseMarker(req, marker) {
    assert.ok(req);
    assert.ok(req.config);
    asssert.ok(req.config.marker);
    assert.ok(marker);

    try {
        var aes;
        var iv = req.config.marker.iv;
        var key = req.config.marker.key;

        aes = crypto.createDecipheriv('aes128', key, iv);
        aes.update(marker, 'base64');

        return parseInt(aes.final('utf8').split('::')[1], 10);
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

    var bucket = req.params.bucket;
    var db = req.db;
    var indexes = [];
    var log = req.log;

    if (!bucket)
        return next(new restify.MissingParameterError('bucket is required'));
    if (req.params.index) {
        if (Array.isArray(req.params.index)) {
            req.params.index.forEach(function (i) {
                indexes.push(i + '');
            });
        } else {
            indexes.push(req.params.index + '');
        }
    }

    log.debug({
        bucket: req.params.bucket,
        indexes: indexes
    }, 'CreateBucket entered');

    return db.buckets(function (err, buckets) {
        if (err)
            return next(err);

        if (buckets[req.params.bucket])
            return next(new errors.BucketAlreadyExistsError(req.params.bucket));

        return db.createBucket(req.params.bucket, indexes, function (err2) {
            if (err2)
                return next(err2);

            log.debug({bucket: bucket}, 'CreateBucket done');
            res.header('Location', '/%s', req.params.bucket);
            res.send(201, {bucket: indexes});
            return next();
        });
    });

}


function getBucket(req, res, next) {
    assert.ok(req.db);

    var db = req.db;
    var log = req.log;

    log.debug({bucket: req.params.bucket}, 'GetBucket entered');
    return db.buckets(function (err, buckets) {
        if (err)
            return next(err);

        if (!buckets[req.params.bucket])
            return next(new errors.ObjectNotFoundError(req.params.bucket));

        req.bucket = {bucket: buckets[req.params.bucket]};
        return next();
    });
}


function listKeys(req, res, next) {
    if (!req.params.keys)
        return next();

    var opts = {
        limit: 1000,
        offset: 0
    };

    try {
        if (req.params.limit)
            opts.limit = paramToNumber('limit', req.params.limit, 1000);
        if (req.params.marker)
            opts.offset = parseMarker(req, req.params.marker);
        if (req.params.prefix)
            opts.prefix = req.params.prefix;
    } catch (e) {
        return next(e);
    }

    return req.db.list(req.params.bucket, opts, function (err, keys) {
        if (err)
            return next(err);

        req.bucket.keys = {};
        req.bucket.numKeys = keys.total;
        if (keys.keys.length < keys.total) {
            var marker = createMarker(req, opts.offset + opts.limit);
            var link = sprintf('/%s?marker=%s', req.params.bucket, )
            res.link(link, 'next');
        }

        keys.keys.forEach(function (k) {
            req.bucket.keys[k.key] = {
                etag: k.etag,
                mtime: k.mtime
            };
        });

        return done();
    });
}


function getBucketDone(req, res, next) {
    req.log.debug({bucket: req.params.bucket, req.bucket}, 'GetBucket done');
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
            return next(new errors.ObjectNotFoundError(req.params.bucket));

        return db.deleteBucket(req.params.bucket, function (err2) {
            if (err2)
                return next(err2);

            log.debug({bucket: req.params.bucket}, 'DeleteBucket done');
            res.send(204);
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

        return server;
    }
};
