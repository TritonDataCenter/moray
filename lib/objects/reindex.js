// Copyright 2014 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var libuuid = require('libuuid');
var vasync = require('vasync');
var clone = require('clone');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');


///--- Globals

var sprintf = util.format;

///--- Handlers

function requiresReindex(req, cb) {
    var active = req.bucket.reindex_active;
    if (active && Object.keys(active).length > 0) {
        req.requiresReindex = true;
    }

    // verify count argument
    if (req.count <= 0) {
        var err = new Error(sprintf('count %d < 1', req.count));
        err.name = 'InvalidArgumentError';
        cb(err);
        return;
    }

    req._processed = 0;
    req._updateRows = [];
    cb(null);
}


function queryRows(req, cb) {
    if (!req.requiresReindex) {
        cb(null);
        return;
    }

    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;


    var ignore = [];
    if (bucket.reindex_active) {
        Object.keys(bucket.reindex_active).forEach(function (ver) {
            ignore = ignore.concat(bucket.reindex_active[ver]);
        });
    }

    var sql = sprintf('SELECT *, \'%s\' AS req_id FROM %s ' +
                      'WHERE _rver IS NULL OR _rver < %d ' +
                      'LIMIT %d FOR UPDATE',
                      req.req_id || 'null', bucket.name,
                      bucket.options.version,
                      req.count);
    log.debug({
        bucket: bucket.name,
        count: req.count
    }, 'reindexRows: entered');

    var q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'reindexRows: failed');
        cb(err);
    });
    q.on('row', function (r) {
        var obj = common.rowToObject(bucket, r, ignore);
        req._updateRows.push(obj);
    });
    q.once('end', function () {
        cb(null);
    });
}


function updateRows(req, cb) {
    if (!req.requiresReindex || req._updateRows.length === 0) {
        cb(null);
        return;
    }

    var log = req.log;
    var pg = req.pg;
    var version = parseInt(req.bucket.options.version, 10);

    function updateRow(obj, callback) {
        var index = common.indexObject(req.bucket.index, obj.value);
        var fields = [];
        var values = [];

        // set _value to merge any changes picked up from updateObjects
        fields.push('_value');
        values.push(obj.value);
        Object.keys(req.bucket.index).forEach(function (k) {
            fields.push(k);
            values.push(index[k]);
        });
        fields.push('_rver');
        values.push(version);

        var fieldSet = fields.map(function (field, idx) {
            return sprintf('%s = $%d', field, idx + 1);
        }).join(', ');

        values.push(obj.key);
        var sql = sprintf('UPDATE %s SET %s ' +
                        'WHERE _key=$%d RETURNING _id,  \'%s\' AS req_id',
                        req.bucket.name,
                        fieldSet,
                        values.length,
                        req.req_id);
        log.trace({
            bucket: req.bucket.name,
            version: version,
            key: obj.key
        }, 'updateRow: entered');
        dtrace['reindexobjects-record'].fire(function () {
            return ([req.msgid, req.bucket.name, obj.key]);
        });


        var q = pg.query(sql, values);
        q.once('error', function (err) {
            log.trace(err, 'updateRow: failed');
            callback(err);
        });
        q.once('end', function () {
            log.trace('updateRow: done');
            callback(null);
        });
    }

    log.debug({
        rowCount: req._updateRows.length
    }, 'updateRows: entered');

    vasync.forEachPipeline({
        inputs: req._updateRows,
        func: updateRow
    }, function (err) {
        if (err) {
            log.debug('updateRows: failed');
        } else {
            log.debug('updateRows: done');
            req._processed = req._updateRows.length;
        }
        delete req._updateRows;
        cb(err);
    });
}


function recordStatus(req, cb) {
    if (!req.requiresReindex) {
        cb(null);
        return;
    }

    var log = req.log;
    var pg = req.pg;

    if (req._processed === 0) {
        // Clear reindex_active if all rows are up to date
        var sql = 'UPDATE buckets_config SET reindex_active = \'{}\' ' +
                  'WHERE name = $1';
        log.debug({
            bucket: req.bucket.name
        }, 'recordStatus: entered');
        var q = pg.query(sql, [req.bucket.name]);
        q.on('error', function (err) {
            log.debug('recordStatus: failed');
            cb(err);
        });
        q.on('end', function () {
            log.debug('recordStatus: done');
            cb();
        });
    } else {
        cb();
    }
}


function countRemaining(req, cb) {
    if (!req.requiresReindex || req.opts.no_count) {
        cb(null);
        return;
    }

    var log = req.log;
    var pg = req.pg;

    var sql = sprintf('SELECT COUNT(*) AS _count FROM %s ' +
                      'WHERE _rver IS NULL OR _rver < %d',
                      req.bucket.name,
                      parseInt(req.bucket.options.version, 10));
    log.debug({
        bucket: req.bucket.name,
        sql: sql
    }, 'countRemaining: entered');

    var q = pg.query(sql);

    q.on('error', function (err) {
        q.removeAllListeners('end');
        q.removeAllListeners('row');
        log.debug(err, 'query error');
        cb(err);
    });
    q.on('row', function (row) {
        log.debug({
            row: row
        }, 'countRemaining: row found');

        req._countRemaining = parseInt(row._count, 10);
    });
    q.on('end', function () {
        q.removeAllListeners('error');
        q.removeAllListeners('row');
        log.debug('countRemaining: done');
        cb();
    });
}



function reindex(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _reindex(name, count, opts, res) {
        var id = opts.req_id || libuuid.create();
        count = parseInt(count, 10);

        dtrace['reindexobjects-start'].fire(function () {
            return ([res.msgid, id, name, count]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: name,
            count: count,
            opts: opts
        }, 'reindexObjects: entered');

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr,
                          'reindexObjects: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'reindexObjects: transaction started');

            var req = {
                bucket: {
                    name: name
                },
                log: log,
                pg: pg,
                manatee: manatee,
                opts: opts,
                req_id: id,
                msgid: res.msgid,
                count: count
            };
            // Forcibly disable bucket cache
            req.opts.noBucketCache = true;

            vasync.pipeline({
                funcs: [
                    common.loadBucket,
                    requiresReindex,
                    queryRows,
                    updateRows,
                    recordStatus,
                    countRemaining
                ],
                arg: req
            }, function (err) {
                if (err) {
                    log.debug(err, 'reindexObjects: failed');
                    pg.rollback();
                    res.end(common.pgError(err));
                    return;
                }

                pg.commit(function (err2) {
                    if (err2) {
                        log.debug(err2, 'reindexObjects: failed');
                        res.end(common.pgError(err2));
                    } else {
                        log.debug('reindexObjects: done');
                        var result = {
                            processed: req._processed
                        };
                        if (req._countRemaining !== undefined) {
                            result.remaining = req._countRemaining;
                        }
                        res.end(result);
                    }

                    dtrace['findobjects-done'].fire(function () {
                        return ([res.msgid, req._processed]);
                    });
                });

                return;
            });
        });
    }
    return (_reindex);
}


///--- Exports

module.exports = {
    reindex: reindex
};
