// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var Cache = require('expiring-lru-cache');
var crc = require('crc');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers


function checkCache(req, cb) {
        req.cacheKey = common.cacheKey(req.bucket.name, req.key);

        if (req.noCache)
                return (cb());

        req.object = req.cache.get(req.cacheKey);
        return (cb());
}


function start(req, cb) {
        if (req.object)
                return (cb());

        var opts = {
                async: !req.noCache,
                read: true
        };
        req.manatee.pg(opts, function (err, client) {
                if (err) {
                        cb(err);
                } else {
                        req.log.debug({
                                pg: client
                        }, 'getObject: client acquired');
                        req.pg = client;
                        cb();
                }
        });

        return (undefined);
}


function loadObject(req, cb) {
        if (req.object)
                return (cb());

        var bucket = req.bucket.name;
        var key = req.key;
        var log = req.log;
        var sql = sprintf('SELECT _id, _key, _value, _etag, _mtime ' +
                          'FROM %s WHERE _key=\'%s\'', bucket, key);

        log.debug({
                bucket: req.bucket.name,
                key: req.key
        }, 'loadObject: entered');

        req.pg.query(sql, function (err, result) {
                if (err) {
                        log.debug(err, 'loadObject: failed');
                        return (cb(err));
                }

                if (result.rows.length === 0)
                        return (cb(new ObjectNotFoundError(bucket, key)));

                req.object = common.rowToObject(bucket, result.rows[0]);
                req.cache.set(req.cacheKey, req.object);
                log.debug({
                        object: req.object
                }, 'loadObject: done');

                return (cb());
        });
        return (undefined);
}



///--- Handlers


function get(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var cache = new Cache({
                name: 'ObjectCache',
                size: (options.size || 1000),
                expiry: (options.expiry || (30 * 1000))
        });
        var manatee = options.manatee;

        function _get(b, k, opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });
                var req = {
                        bucket: {
                                name: b
                        },
                        cache: cache,
                        key: k,
                        log: log,
                        noCache: opts.noCache,
                        manatee: manatee
                };

                log.debug({
                        bucket: b,
                        key: k,
                        opts: opts
                }, 'getObject: entered');

                vasync.pipeline({
                        funcs: [
                                checkCache,
                                start,
                                loadObject
                        ],
                        arg: req
                }, function (err) {
                        if (err) {
                                log.warn(err, 'get: failed');
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
                                req.pg.rollback(function () {});
                });
        }

        return (_get);
}



///--- Exports

module.exports = {
        get: get
};
