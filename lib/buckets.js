// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var crypto = require('crypto');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;
var INDEX_TYPES = {string: true, number: true, 'boolean': true};


///--- Helpers

function paramToNumber(name, param, max) {
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


function paginationOptions(req) {
    var opts;

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

    return opts;
}



///--- Routes

function list(req, res, next) {
    var db = req.bucketManager;
    var log = req.log;

    log.debug('ListBuckets entered');
    return db.list(function (err, buckets) {
        if (err)
            return next(err);

        Object.keys(buckets).forEach(function (k) {
            buckets[k].pre = buckets[k].pre.map(function (p) {
                return p.toString();
            });
            buckets[k].post = buckets[k].post.map(function (p) {
                return p.toString();
            });
        });

        log.debug({buckets: buckets}, 'ListBuckets done');
        res.send(buckets);
        return next();
    });
}


function put(req, res, next) {
    var db = req.bucketManager;
    var fn;
    var log = req.log;
    var post = [];
    var pre = [];
    var schema = req.params.schema || {};

    (req.params.pre || []).forEach(function (p) {
        pre.push(eval('fn = ' + p));
    });

    (req.params.post || []).forEach(function (p) {
        post.push(eval('fn = ' + p));
    });
    if (fn) {
        fn = null;
    } // make javascriptlint shutup

    log.debug({
        bucket: req.params.bucket,
        schema: schema,
        pre: pre,
        post: post
    }, 'PutBucket entered');

    var opts = {
        schema: schema,
        pre: pre,
        post: post
    };
    return db.put(req.params.bucket, opts, function (err) {
        if (err)
            return next(err);

        log.debug({bucket: req.params.bucket}, 'PutBucket done');
        res.send(204);
        return next();
    });
}


function getBucketSchema(req, res, next) {
    var db = req.bucketManager;
    var log = req.log;

    req.bucket = {};
    if (req.params.schema === false || req.params.schema === 'false')
        return next();

    log.debug({bucket: req.params.bucket}, 'GetBucketSchema entered');
    return db.get(req.params.bucket, function (err, bucket) {
        if (err)
            return next(err);

        req.bucket = bucket;
        req.bucket.pre = req.bucket.pre.map(function (p) {
            return p.toString();
        });
        req.bucket.post = req.bucket.post.map(function (p) {
            return p.toString();
        });

        return next();
    });
}


function listKeys(req, res, next) {
    assert.ok(req.bucket);

    if (req.params.keys === false || req.params.keys === 'false')
        return next();

    var db = req.bucketManager;
    var opts = paginationOptions(req);

    return db.keys(req.params.bucket, opts, function (err, keys) {
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
    var db = req.bucketManager;
    var log = req.log;

    log.debug({bucket: req.params.bucket}, 'DeleteBucket entered');
    return db.del(req.params.bucket, function (err) {
        if (err)
            return next(err);

        log.debug({bucket: req.params.bucket}, 'DeleteBucket done');
        res.send(204);
        return next();
    });
}



///--- Exports

module.exports = {
    mount: function mount(server) {
        args.assertArgument(server, 'object', server);

        server.get({path: '/', name: 'ListBuckets'}, list);
        server.head({path: '/', name: 'ListBuckets'}, list);

        var bodyParser = restify.bodyParser();
        server.put({path: '/:bucket', name: 'PutBucket'}, bodyParser, put);

        server.get({path: '/:bucket', name: 'GetBucket'},
                   getBucketSchema, listKeys, getBucketDone);
        server.head({path: '/:bucket', name: 'GetBucket'},
                    getBucketSchema, listKeys, getBucketDone);

        server.del({path: '/:bucket', name: 'DeleteBucket'}, del);

        return server;
    }
};
