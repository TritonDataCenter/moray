// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var microtime = require('microtime');
var crc = require('crc');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;

// This is exported so that batch put can leverage it
var PIPELINE = [
        common.loadBucket,
        common.selectForUpdate,
        runPreChain,
        common.checkEtag,
        indexObject,
        getNextId,
        insert,
        update,
        updateSerial,
        common.runPostChain
];



///--- Internal Functions

function _createEtag(req) {
        return (crc.hex32(crc.crc32(req._value)));
}


function _now() {
        return (Math.round((microtime.now()/1000)));
}

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
                        if (t === 'boolean') {
                                ndx[k] = v.toString().toUpperCase();
                        } else {
                                ndx[k] = v.toString();
                        }
                        break;
                case 'number':
                        if (t === 'number') {
                                ndx[k] = parseInt(v, 10);
                        } else {
                                ndx[k] = v.toString();
                        }
                        break;
                case 'string':
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
                value: req.value,
                headers: req.headers || {}
        };
        var log = req.log;

        log.debug({
                bucket: req.bucket.name,
                key: req.key,
                value: req.value
        }, 'runPreChain: entered');

        vasync.pipeline({
                funcs: req.bucket.pre,
                arg: cookie
        }, function (err) {
                if (err) {
                        log.debug(err, 'runPreChain: fail');
                        cb(err);
                } else {
                        req.key = cookie.key;
                        req.value = cookie.value;
                        req._value = JSON.stringify(req.value);

                        log.debug({
                                bucket: req.bucket.name,
                                key: req.key,
                                value: req.value
                        }, 'runPreChain: done');
                        cb();
                }
        });
        return (undefined);
}


function indexObject(req, cb) {
        try {
                req.index = _indexObject(req.bucket.index, req.value);
                req.log.debug({
                        bucket: req.bucket.name,
                        index: req.index
                }, 'indexObject: done (indexed)');
        } catch (e) {
                return (cb(e));
        }

        return (cb());
}


function getNextId(req, cb) {
        if (req.bucket.options.guaranteeOrder !== true) {
                cb();
                return;
        }

        var b = req.bucket.name;
        var id;
        var log = req.log;
        var q;
        var sql = sprintf('SELECT id, \'%s\' AS req_id ' +
                          'FROM %s_locking_serial FOR UPDATE',
                          req.req_id, b);

        log.debug({
                bucket: req.bucket.name,
                key: req.key
        }, 'getNextId: entered');

        q = req.pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'getNextId: failed');
                cb(err);
        });

        q.once('row', function (r) {
                id = r.id + 1;
        });

        q.once('end', function () {
                assert.ok(id, 'id not found');
                req.value._txn_snap = id;
                log.debug({id: req.value._txn_snap}, 'getNextId: done');
                cb();
        });

}


function insert(req, cb) {
        if (req.previous) {
                cb();
                return;
        }

        req.log.debug({
                bucket: req.bucket,
                etag: req.etag,
                vnode: req.vnode,
                key: req.key,
                value: req.value
        }, 'insert: entered');

        var id;
        var keyStr = '';
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql;
        var valStr = '';

        req.res._etag = _createEtag(req);
        var values = [req.key, req._value, req.res._etag, _now(),
                      req.vnode];

        if (req.bucket.options.guaranteeOrder === true) {
                values.push(req.value._txn_snap);
                keyStr += ', _txn_snap';
                valStr += ', $' + values.length;
        }

        Object.keys(req.index).forEach(function (k) {
                values.push(req.index[k]);
                keyStr += ', ' + k;
                valStr += ', $' + values.length;
        });

        sql = sprintf('INSERT INTO %s (_key, _value, _etag, _mtime, _vnode%s) ' +
                      'VALUES ($1, $2, $3, $4, $5%s) ' +
                      'RETURNING _id, \'%s\' AS req_id',
                      req.bucket.name, keyStr, valStr, req.req_id);

        q = pg.query(sql, values);

        q.once('error', function (err) {
                log.debug(err, 'insert: failed');
                cb(err);
        });

        q.once('row', function (r) {
                id = r._id;
        });

        q.once('end', function () {
                req.value._id = id;
                assert.ok(req.value._id);
                log.debug({
                        objectId: req.value._id,
                        _txn_snap: req.value._txn_snap
                }, 'insert: done');
                cb();
        });
}


function update(req, cb) {
        if (!req.previous) {
                cb();
                return;
        }

        var extraStr = '';
        var lock = (req.bucket.options.guaranteeOrder === true);
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql;
        var track = (req.bucket.options.trackModification === true);
        var _txn_snap = req.value._txn_snap;

        req.res._etag = _createEtag(req);
        var vals = [req._value, req.res._etag, req.vnode];

        Object.keys(req.bucket.index).forEach(function (k) {
                extraStr += ', ' + k + '=';
                if (req.index[k] !== undefined) {
                        extraStr += '$' + (vals.length + 1);
                        switch (req.bucket.index[k].type) {
                        case 'boolean':
                                var v = req.index[k].toString().toUpperCase();
                                vals.push(v);
                                break;
                        case 'number':
                                vals.push(parseInt(req.index[k], 10));
                                break;
                        case 'string':
                                vals.push(req.index[k]);
                                break;
                        default:
                                break;
                        }
                } else {
                        extraStr += 'NULL';
                }
        });

        sql = sprintf('UPDATE %s SET _value=$1, _etag=$2%s, _vnode=$3, ' +
                      '_mtime=%d %s %s ' +
                      'WHERE _key=\'%s\' RETURNING _id,  \'%s\' AS req_id',
                      req.bucket.name,
                      extraStr,
                      _now(),
                      (track ? ', _id=DEFAULT' : ''),
                      (lock ? ', _txn_snap=' + _txn_snap : ''),
                      req.key, req.req_id);

        req.log.debug({
                bucket: req.bucket.name,
                etag: req.etag,
                key: req.key,
                index: req.index,
                value: req.value,
                sql: sql
        }, 'update: entered');

        q = pg.query(sql, vals);
        q.once('error', function (err) {
                log.debug(err, 'update: failed');
                cb(err);
        });
        q.once('end', function () {
                log.debug('update: done');
                cb();
        });
}


function updateSerial(req, cb) {
        if (req.bucket.options.guaranteeOrder !== true) {
                cb();
                return;
        }

        var b = req.bucket.name;
        var id = req.value._txn_snap;
        var log = req.log;
        var q;
        var sql;

        log.debug({
                bucket: req.bucket.name,
                key: req.key,
                id: id
        }, 'updateSerial: entered');

        sql = sprintf('UPDATE %s_locking_serial SET id=%d ' +
                      'RETURNING \'%s\' AS req_id',
                      b, id, req.req_id);
        q = req.pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'updateSerial: failed');
                cb(err);
        });

        q.once('end', function () {
                log.debug({id: id}, 'updateSerial: done');
                cb();
        });
}





function put(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _put(b, k, v, opts, res) {
                var id = opts.req_id || uuid.v1();

                dtrace['putobject-start'].fire(function () {
                        return ([res.msgid, id, b, k, opts._value]);
                });

                var log = options.log.child({
                        req_id: id
                });

                log.debug({
                        bucket: b,
                        key: k,
                        value: v,
                        opts: opts
                }, 'putObject: entered');

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'putObject: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'putObject: transaction started');

                        vasync.pipeline({
                                funcs: PIPELINE,
                                arg: {
                                        bucket: {
                                                name: b
                                        },
                                        etag: opts.etag,
                                        key: k,
                                        log: log,
                                        pg: pg,
                                        manatee: manatee,
                                        req_id: id,
                                        res: res,
                                        value: v,
                                        _value: opts._value,
                                        vnode: opts.vnode || null,
                                        headers: opts.headers || {}
                                }
                        }, common.pipelineCallback({
                                log: log,
                                name: 'putObject',
                                pg: pg,
                                res: res
                        }));
                });
        }

        return (_put);
}



///--- Exports

module.exports = {
        put: put,
        pipeline: PIPELINE
};
