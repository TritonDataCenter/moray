// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;



///--- Routes

function put(req, res, next) {
    assert.ok(req.db);

    var body = (typeof (req.body) === 'object' ? (req.body || {}) : {});
    var bucket = req.params.bucket;
    var db = req.db;
    var key = req.params.key;
    var log = req.log;

    log.debug({bucket: bucket, key: key}, 'PutObject entered, body=%j', body);
    return db.put(bucket, key, body, function (err, etag) {
        if (err)
            return next(err);

        log.debug({bucket: bucket, key: key}, 'PutObject done');
        res.etag = etag;
        res.send(204);
        return next();
    });
}


function get(req, res, next) {
    assert.ok(req.db);

    var bucket = req.params.bucket;
    var db = req.db;
    var key = req.params.key;
    var log = req.log;

    log.debug({bucket: bucket, key: key}, 'GetObject entered');
    return db.get(bucket, key, function (err, obj) {
        if (err)
            return next(err);

        res.etag = obj.etag;
        res.header('Last-Modified', obj.mtime);

        log.debug({bucket: bucket, key: key}, 'GetObject -> %j', obj.data);
        res.send(obj.data);
        return next();
    });
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
        server.put({path: '/:bucket/:key', name: 'PutObject'}, bodyParser, put);

        server.get({path: '/:bucket/:key', name: 'GetObject'}, get);
        server.head({path: '/:bucket/:key', name: 'HeadObject'}, get);

        server.del({path: '/:bucket/:key', name: 'DeleteObject'}, del);

        server.post({path: '/:bucket/:key', name: 'RestoreObject'}, restore);
        return server;
    }

};
