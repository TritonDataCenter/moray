// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var LRU = require('lru-cache');
var crc = require('crc');
var once = require('once');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers


function checkCache(req, cb) {
        req.cacheKey = common.cacheKey(req.bucket.name, req.key);

        if (req.noCache) {
                cb();
                return;
        }

        req.object = req.cache.get(req.cacheKey);
        cb();
}


function start(req, cb) {
        if (req.object) {
                cb();
                return;
        }

        var opts = {
                async: false,
                read: true
        };
        req.manatee.pg(opts, function (err, client) {
                if (err) {
                        cb(err);
                } else {

                        if (opts.timeout)
                                client.setTimeout(opts.timeout);

                        req.log.debug({
                                pg: client
                        }, 'getObject: client acquired');
                        req.pg = client;
                        cb();
                }
        });
}


function loadObject(req, cb) {
        if (req.object) {
                cb();
                return;
        }

        var bucket = req.bucket.name;
        var log = req.log;
        var q;
        var row;
        var sql = sprintf('SELECT *, \'%s\' AS req_id ' +
                          'FROM %s WHERE _key=$1',
                          req.req_id, bucket);

        cb = once(cb);

        log.debug({
                bucket: req.bucket.name,
                key: req.key
        }, 'loadObject: entered');

        q = req.pg.query(sql, [req.key]);

        q.once('error', function (err) {
                log.debug(err, 'loadObject: failed');
                cb(err);
        });

        q.once('row', function (r) {
                row = r;
        });

        q.once('end', function () {
                if (!row) {
                        cb(new ObjectNotFoundError(bucket, req.key));
                } else {
                        req.object = common.rowToObject(req.bucket, row);
                        req.cache.set(req.cacheKey, req.object);
                        log.debug({
                                object: req.object
                        }, 'loadObject: done');
                        cb();
                }
        });
}



///--- Handlers


function get(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var cache = new LRU({
                name: 'ObjectCache',
                max: (options.size || 1000),
                maxAge: (options.expiry || (30 * 1000))
        });
        var manatee = options.manatee;

        function _get(b, k, opts, res) {
                var id = opts.req_id || uuid.v1();
                var log = options.log.child({
                        req_id: id
                });
                var req = {
                        bucket: {
                                name: b
                        },
                        cache: cache,
                        key: k,
                        log: log,
                        noCache: opts.noCache,
                        manatee: manatee,
                        req_id: id,
                        timeout: opts.timeout
                };

                dtrace['getobject-start'].fire(function () {
                        return ([res.msgid, id, b, k]);
                });

                log.debug({
                        bucket: b,
                        key: k,
                        opts: opts
                }, 'getObject: entered');

                vasync.pipeline({
                        funcs: [
                                checkCache,
                                start,
                                function _loadBucket(r, cb) {
                                        if (r.object) {
                                                cb();
                                        } else {
                                                common.loadBucket(r, cb);
                                        }
                                },
                                loadObject
                        ],
                        arg: req
                }, function (err) {
                        if (err) {
                                log.debug(err, 'get: failed');
                                res.end(err);
                        } else if (!req.object) {
                                log.debug('get: object not found');
                                err = new ObjectNotFoundError(b, k);
                                res.end(err);
                        } else {
                                log.debug({
                                        bucket: b,
                                        key: k,
                                        value: req.object
                                }, 'get: done');
                                res.end(req.object);
                        }

                        if (req.pg)

                                req.pg.release();
                        dtrace['getobject-done'].fire(function () {
                                var val = JSON.stringify(req.object);
                                return ([res.msgid, val]);
                        });
                });
        }

        return (_get);
}



///--- Exports

module.exports = {
        get: get
};
