/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var crc = require('crc');
var vasync = require('vasync');

var control = require('../control');
var common = require('./common');
var dtrace = require('../dtrace');


// --- Globals

var ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'key', type: 'string' },
    { name: 'value', type: 'object' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    common.loadBucket,
    common.selectForUpdate,
    common.verifyBucket,
    runPreChain,
    common.checkEtag,
    indexObject,
    getNextId,
    insert,
    update,
    updateSerial,
    common.runPostChain
];

// This is exported fo batch 'put' operations
var SUBPIPELINE = PIPELINE.slice(1);


// --- Internal Functions

function _createEtag(req) {
    return (crc.hex32(crc.crc32(req._value)));
}


function _now() {
    return (Date.now());
}


// --- Handlers

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
    var sql = util.format(('SELECT id, \'%s\' AS req_id ' +
                           'FROM %s_locking_serial FOR UPDATE'),
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

    // Prep _value for insert/update
    req._value = JSON.stringify(req.value);
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

    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;
    var row;

    req._etag = _createEtag(req);
    var fields = ['_key', '_value', '_etag', '_mtime', '_vnode'];
    var values = [req.key, req._value, req._etag, _now(), req.value.vnode];
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
    var valStr = values.map(function (_, idx) {
        return util.format('$%d', idx + 1);
    }).join(', ');

    sql = [
        'INSERT INTO ' + req.bucket.name,
        '(' + keyStr + ') VALUES (' + valStr + ')',
        'RETURNING _id,'
    ];
    if (req.bucket.hasExtendedId) {
        sql.push('_idx, ');
    }
    sql.push('\'' + req.req_id + '\' AS req_id');

    q = pg.query(sql.join(' '), values);

    q.once('error', function (err) {
        log.debug(err, 'insert: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        try {
            req.value._id = common.rowExtractId(req.bucket, req.key, row);
        } catch (ex) {
            log.error(ex, 'insert: error converting "_id" value');
            cb(ex);
            return;
        }
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

    assert.bool(req.bucket.hasExtendedId, 'req.bucket.hasExtendedId');

    var log = req.log;
    var pg = req.pg;
    var q;
    var sql;

    var lock = (req.bucket.options.guaranteeOrder === true);
    var _txn_snap = req.value._txn_snap;

    req._etag = _createEtag(req);

    var fields = ['_value', '_etag', '_vnode', '_mtime'];
    var values = [req._value, req._etag, req.value.vnode, _now()];
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
        return util.format('%s = $%d', field, idx + 1);
    }).join(', ');

    values.push(req.key);

    sql = [
        'UPDATE ' + req.bucket.name,
        'SET ' + fieldSet
    ];
    if (lock) {
        sql.push(', _txn_snap =', _txn_snap);
    }
    sql.push('WHERE _key = $' + values.length);
    sql.push('RETURNING \'' + req.req_id + '\' AS req_id');

    sql = sql.join(' ');

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

    sql = util.format(('UPDATE %s_locking_serial SET id=%d ' +
                       'RETURNING \'%s\' AS req_id'),
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


// --- API

function put(options) {
    control.assertOptions(options);

    function _put(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var bucket = argv[0];
        var key = argv[1];
        var value = argv[2];
        var opts = argv[3];

        var req = control.buildReq(opts, rpc, options);
        req.bucket = {
            name: bucket
        };
        req.key = key;
        req.value = value;
        req._value = opts._value;
        req.etag = opts.etag;

        dtrace['putobject-start'].fire(function () {
            return ([req.msgid, req.req_id, bucket, key, opts._value]);
        });
        req.log.debug({
            bucket: bucket,
            key: key,
            value: value,
            opts: opts
        }, 'putObject: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: common.stdOutput(req)
        });
    }

    return (_put);
}


// --- Exports

module.exports = {
    put: put,
    pipeline: SUBPIPELINE
};
