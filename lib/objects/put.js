/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var microtime = require('microtime');
var crc = require('crc');
var libuuid = require('libuuid');
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


///--- Handlers

function runPreChain(req, cb) {
    if (req.bucket.pre.length === 0) {
        cb();
        return;
    }

    var cookie = {
        bucket: req.bucket.name,
        key: req.key,
        log: req.log,
        pg: req.pg,
        schema: req.bucket.index,
        value: req.value,
        headers: req.headers || {},
        update: (req.previous) ? true : false
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
            return;
        }

        req.key = cookie.key;
        req.value = cookie.value;
        req._value = JSON.stringify(req.value);

        log.debug({
            bucket: req.bucket.name,
            key: req.key,
            value: req.value
        }, 'runPreChain: done');
        cb();
    });
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


function indexObject(req, cb) {
    try {
        req.index = common.indexObject(req.bucket.index, req.value);
        req.log.debug({
            bucket: req.bucket.name,
            index: req.index
        }, 'indexObject: done (indexed)');
    } catch (e) {
        return (cb(e));
    }

    return (cb());
}


function insert(req, cb) {
    if (req.previous) {
        cb();
        return;
    }

    req.log.debug({
        bucket: req.bucket,
        etag: req.etag,
        vnode: req.value.vnode,
        key: req.key,
        value: req.value
    }, 'insert: entered');

    var id;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;

    req.res._etag = _createEtag(req);
    var fields = ['_key', '_value', '_etag', '_mtime', '_vnode'];
    var values = [req.key, req._value, req.res._etag, _now(), req.value.vnode];
    if (req.bucket.options.guaranteeOrder === true) {
        fields.push('_txn_snap');
        values.push(req.value._txn_snap);
    }
    if (req.bucket.reindex_active) {
        /* set _rver if table undergoing reindexing */
        fields.push('_rver');
        values.push(parseInt(req.bucket.options.version, 10));
    }
    Object.keys(req.index).forEach(function (k) {
        fields.push(k);
        values.push(req.index[k]);
    });

    var keyStr = fields.join(', ');
    var valStr = values.map(function (val, idx) {
        return sprintf('$%d', idx+1);
    }).join(', ');

    sql = sprintf('INSERT INTO %s (%s) VALUES (%s) ' +
                  'RETURNING _id, \'%s\' AS req_id',
                  req.bucket.name, keyStr, valStr,
                  req.req_id);

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

    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;

    var lock = (req.bucket.options.guaranteeOrder === true);
    var track = (req.bucket.options.trackModification === true);
    var _txn_snap = req.value._txn_snap;

    req.res._etag = _createEtag(req);

    var fields = ['_value', '_etag', '_vnode', '_mtime'];
    var values = [req._value, req.res._etag, req.value.vnode, _now()];
    if (req.bucket.reindex_active) {
        /* update _rver if table undergoing reindexing */
        fields.push('_rver');
        values.push(parseInt(req.bucket.options.version, 10));
    }
    Object.keys(req.bucket.index).forEach(function (k) {
        fields.push(k);
        values.push(req.index[k]);
    });

    var fieldSet = fields.map(function (field, idx) {
        return sprintf('%s = $%d', field, idx + 1);
    }).join(', ');

    values.push(req.key);
    sql = sprintf('UPDATE %s SET %s %s %s ' +
                  'WHERE _key=$%d RETURNING _id,  \'%s\' AS req_id',
                  req.bucket.name,
                  fieldSet,
                  (track ? ', _id=DEFAULT' : ''),
                  (lock ? ', _txn_snap=' + _txn_snap : ''),
                  values.length,
                  req.req_id);

    req.log.debug({
        bucket: req.bucket.name,
        etag: req.etag,
        key: req.key,
        index: req.index,
        value: req.value,
        vals: values,
        sql: sql
    }, 'update: entered');

    q = pg.query(sql, values);
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
        var id = opts.req_id || libuuid.create();

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

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

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
