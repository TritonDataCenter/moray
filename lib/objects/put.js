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



///--- Internal Functions

function _indexObject(schema, object) {
        assert.object(schema, 'schema');
        assert.object(object, 'object');

        var ndx = {};

        if (Object.keys(schema).length === 0)
                return (ndx);

        function _index(k, v, t) {
                if (v !== undefined && typeof (v) !== t) {
                        if (typeof (v) === 'object') {
                                if (Array.isArray(v)) {
                                        v.forEach(function (v2) {
                                                _index(k, v2, t);
                                        });
                                        return (undefined);
                                } else if (v === null) {
                                        return (false);
                                } else {
                                        throw new InvalidIndexTypeError(k, t);
                                }
                        }
                }

                switch (typeof (v)) {
                case 'boolean':
                case 'string':
                case 'number':
                        ndx[k] = v + '';
                        break;
                default:
                        break;
                }

                return (true);
        }

        Object.keys(schema).forEach(function (k) {
                _index(k, object[k], schema[k].type);
        });

        return (ndx);
}



///--- Handlers

function runPreChain(req, cb) {
        if (req.bucket.pre.length === 0)
                return (cb());

        var cookie = {
                bucket: req.bucket.name,
                key: req.key,
                log: req.log,
                pg: req.pg,
                schema: req.bucket.index,
                value: req.value
        };
        var log = req.log;

        log.debug('runPreChain: entered');

        vasync.pipeline({
                funcs: req.bucket.pre,
                arg: cookie
        }, function (err) {
                if (err) {
                        log.debug(err, 'runPreChain: fail');
                        next(err);
                } else {
                        req.bucket = cookie.bucket;
                        req.key = cookie.key;
                        req.value = cookie.value;

                        log.debug({
                                bucket: req.bucket,
                                key: req.key,
                                value: req.value
                        }, 'runPreChain: done');
                        next();
                }
        });
        return (undefined);
}


function createEtag(req, cb) {
        req.etag = crc.hex32(crc.crc32(req._value));
        cb();
}


function indexObject(req, cb) {
        try {
                req.index = _indexObject(req.bucket.index, req.value);
                req.log.debug({
                        bucket: req.bucket.name,
                        schema: req.bucket.index,
                        index: req.index
                }, 'indexObject: done (indexed)');
        } catch (e) {
                return (cb(e));
        }

        return (cb());
}


function insert(req, cb) {
        if (req.previous)
                return (cb());

        req.log.debug({
                bucket: req.bucket,
                etag: req.etag,
                key: req.key,
                value: req.value
        }, 'insert: entered');

        var i = 0;
        var keyStr = '';
        var log = req.log;
        var opts;
        var pg = req.pg;
        var sql;
        var valStr = '';
        var values = [req.key, req._value, req.etag];

        Object.keys(req.index).forEach(function (k) {
                values.push(req.index[k]);
                keyStr += ', ' + k;
                valStr += ', $' + values.length;
        });

        sql = sprintf('INSERT INTO %s (_key, _value, _etag%s) ' +
                      'VALUES ($1, $2, $3%s) RETURNING _id',
                      req.bucket.name, keyStr, valStr),

        pg.query(sql, values, function (err, result) {
                if (err) {
                        log.debug(err, 'insert: failed');
                        cb(err);
                } else {
                        req.value._id = result.rows[0]._id;
                        log.debug({
                                objectId: req.value._id
                        }, 'insert: done');
                        cb();
                }
        });
        return (undefined);
}


function update(req, cb) {
        if (!req.previous)
                return (cb());

        req.log.debug({
                bucket: req.bucket.name,
                etag: req.etag,
                key: req.key,
                index: req.index,
                value: req.value
        }, 'update: entered');

        var extraStr = '';
        var log = req.log;
        var pg = req.pg;
        var sql;

        Object.keys(req.bucket.index).forEach(function (k) {
                extraStr += ', ' + k + '=';
                if (req.index[k]) {
                        switch (req.bucket.index[k].type) {
                        case 'boolean':
                                extraStr +=
                                req.index[k].toString().toUpperCase();
                                break;
                        case 'number':
                                extraStr += parseInt(req.index[k], 10);
                                break;
                        case 'string':
                                extraStr += '\'' + req.index[k] + '\'';
                                break;
                        default:
                                break;
                        }
                } else {
                        extraStr += 'NULL';
                }
        });

        sql = sprintf('UPDATE %s SET _value=\'%s\', _etag=\'%s\'%s' +
                      ' WHERE _key=\'%s\'',
                      req.bucket.name,
                      req._value,
                      req.etag,
                      extraStr,
                      req.key);

        pg.query(sql, function (err) {
                log.debug(err, 'update: %s', (err ? 'failed' : 'done'));
                cb(err);
        });
        return (undefined);
}


function put(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.pg, 'options.pg');

        var pg = options.pg;

        function _put(opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        opts: opts
                }, 'putObject: entered');

                pg.start(function (err, client) {
                        log.debug({
                                pg: client
                        }, 'putObject: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        common.loadBucket,
                                        common.selectForUpdate,
                                        runPreChain,
                                        indexObject,
                                        createEtag,
                                        insert,
                                        update,
                                        common.runPostChain,
                                        common.commit
                                ],
                                arg: {
                                        bucket: {
                                                name: opts.bucket
                                        },
                                        key: opts.key,
                                        log: log,
                                        pg: client,
                                        pgPool: pg,
                                        value: opts.value,
                                        _value: opts._value
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'put: failed');
                                        pg.rollback(client, function () {
                                                res.end(err);
                                        });
                                } else {
                                        log.debug('put: done');
                                        res.end();
                                }
                        });
                });
        }

        return (_put);
}



///--- Exports

module.exports = {
        put: put
};
