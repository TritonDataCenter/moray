// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;



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


function get(req, res, next) {
    assert.ok(req.db);

    var db = req.db;
    var log = req.log;

    log.debug({bucket: req.params.bucket}, 'GetBucket entered');
    return db.buckets(function (err, buckets) {
        if (err)
            return next(err);

        if (!buckets[req.params.bucket])
            return next(new errors.ObjectNotFoundError(req.params.bucket));

        var bucket = {bucket: buckets[req.params.bucket]};
        log.debug({
            bucket: req.params.bucket,
            body: bucket
        }, 'GetBucket done');
        res.header('Location', '/%s', req.params.bucket);
        res.send(bucket);
        return next();
    });
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

        server.get({path: '/:bucket', name: 'GetBucket'}, get);
        server.head({path: '/:bucket', name: 'GetBucket'}, get);

        server.del({path: '/:bucket', name: 'DeleteBucket'}, del);

        return server;
    }
};
