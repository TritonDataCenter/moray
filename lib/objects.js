// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;



///--- Helpers

function load(req, res, next) {
    assert.ok(req.db);

    var bucket = req.params.bucket;
    var db = req.db;
    var key = req.params.key;
    var log = req.log;
    var tombstone = /.*\/tombstone$/.test(req.path);

    log.debug({
        bucket: bucket,
        key: key,
        tombstone: tombstone
    }, 'load entered');
    return db.get(bucket, key, tombstone, function (err, obj) {
        if (err) {
            if (err.name === 'ResourceNotFoundError' && req.method === 'PUT')
                return next();

            return next(err);
        }

        res.etag = obj.etag;
        res.header('Last-Modified', obj.mtime);

        req.bucket = bucket;
        req.key = key;
        req.object = obj.data;
        log.debug({
            bucket: req.bucket,
            key: req.key
        }, 'load -> %j', req.object);
        return next();
    });
}



///--- Routes

function put(req, res, next) {
    assert.ok(req.db);

    var body = (typeof (req.body) === 'object' ? (req.body || {}) : {});
    var bucket = req.params.bucket;
    var db = req.db;
    var etag;
    var key = req.params.key;
    var log = req.log;

    log.debug({
        bucket: bucket,
        key: key,
        body: body
    }, 'PutObject entered');

    if (req.header('if-match')) {
        etag = {
            match: true,
            value: req.header('if-match')
        };
    } else if (req.header('if-none-match')) {
        etag = {
            match: false,
            value: req.header('if-none-match')
        }
    } else {
        etag = {};
    }

    return db.put(bucket, key, body, etag, function (err, etag2) {
        if (err)
            return next(err);

        log.debug({
            bucket: bucket,
            key: key,
            body: body
        }, 'PutObject done');

        res.etag = etag2;
        res.send(204);
        return next();
    });
}


function get(req, res, next) {
    assert.ok(req.object);

    req.log.debug({
        bucket: req.params.bucket,
        key: req.params.key
    }, 'GetObject  -> %j', req.object);
    res.send(req.object);
    return next();
}


function del(req, res, next) {
    assert.ok(req.db);

    var bucket = req.params.bucket;
    var db = req.db;
    var key = req.params.key;
    var log = req.log;

    log.debug({bucket: bucket, key: key}, 'DeleteObject entered');
    return db.del(bucket, key, function (err) {
        if (err)
            return next(err);

        log.debug({bucket: bucket, key: key}, 'DeleteObject done');
        res.send(204);
        return next();
    });
}


function restore(req, res, next) {
    assert.ok(req.db);

    var bucket = req.params.bucket;
    var db = req.db;
    var key = req.params.key;
    var log = req.log;

    log.debug({bucket: bucket, key: key}, 'RestoreObject entered');
    return db.restore(bucket, key, function (err, obj) {
        if (err)
            return next(err);

        res.etag = obj.etag;
        res.header('Last-Modified', obj.mtime);

        log.debug({bucket: bucket, key: key}, 'RestoreObject -> %j', obj.data);
        res.send(obj.data);
        return next();
    });
}


///--- Exports

module.exports = {
    mount: function mount(server) {
        args.assertArgument(server, 'object', server);

        var bodyParser = restify.bodyParser({
            mapParams: false
        });
        var pre = restify.conditionalRequest();

        server.put({path: '/:bucket/:key', name: 'PutObject'},
                   bodyParser, load, pre, put);

        server.get({path: '/:bucket/:key', name: 'GetObject'},
                   load, pre, get);
        server.head({path: '/:bucket/:key', name: 'HeadObject'},
                    load, pre, get);

        server.del({path: '/:bucket/:key', name: 'DeleteObject'},
                   load, pre, del);

        server.get({
            path: '/:bucket/:key/tombstone',
            name: 'GetObjectTombstone'
        }, load, pre, get);
        server.head({
            path: '/:bucket/:key/tombstone',
            name: 'HeadObjectTombstone'
        }, load, pre, get);

        server.post({
            path: '/:bucket/:key/tombstone',
            name: 'RestoreObject'
        }, load, pre, restore);

        return server;
    }

};
