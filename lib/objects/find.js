// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var ldap = require('ldapjs');
var libuuid = require('libuuid');
var vasync = require('vasync');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers

function getCount(req, cb) {
    if (req.opts.no_count) {
        cb();
        return;
    }

    var bucket = req.bucket.name;
    var log = req.log;
    var sql = sprintf('SELECT count(1) over () as _count, ' +
                      '\'%s\' AS req_id FROM %s %s',
                      req.req_id, bucket, req.where.clause);

    log.debug({
        bucket: req.bucket.name,
        key: req.key,
        sql: sql,
        args: req.where.args
    }, 'getCount: entered');

    var query = req.pg.query(sql, req.where.args);
    query.on('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('row');
        log.debug(err, 'query error');
        cb(err);
    });

    query.on('row', function (row) {
        log.debug({
            row: row
        }, 'getRecords: row found');

        req._count = parseInt(row._count, 10);
    });

    query.on('end', function () {
        query.removeAllListeners('error');
        query.removeAllListeners('row');
        log.debug('getCount: done');
        cb();
    });
}


function getRecords(req, cb) {
    var bucket = req.bucket.name;
    var filter = req.filter;
    var log = req.log;
    var res = req.res;
    var sql = sprintf('SELECT *, \'%s\' AS req_id FROM %s %s',
                      req.req_id, bucket, req.where.clause);

    log.debug({
        bucket: req.bucket.name,
        key: req.key,
        sql: sql,
        args: req.where.args
    }, 'getRecords: entered');

    var query = req.pg.query(sql, req.where.args);
    query.on('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('row');
        log.debug(err, 'query error');
        cb(err);
    });

    query.on('row', function (row) {
        log.debug({
            row: row
        }, 'getRecords: row found');

        // MANTA-317: we need to check that the object actually matches
        // the real filter, which requires us to do this janky stub out
        // of _id and _mtime
        var obj = common.rowToObject(req.bucket, row);

        var v = obj.value;
        v._id = obj._id;
        v._txn_snap = obj._txn_snap;
        v._etag = obj._etag;
        v._mtime = obj._mtime;
        v._count = obj._count = req._count;

        if (filter.matches(v)) {
            delete v._id;
            delete v._txn_snap;
            delete v._etag;
            delete v._mtime;
            delete v._count;
            res.write(obj);
            dtrace['findobjects-record'].fire(function () {
                return ([res.msgid,
                         obj.key,
                         obj._id,
                         obj._etag,
                         row._value]);
            });
            res._num_records++;
        }
    });

    query.on('end', function () {
        query.removeAllListeners('error');
        query.removeAllListeners('row');
        log.debug('getRecords: done');
        cb();
    });
}


function find(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _find(b, f, opts, res) {
        var id = opts.req_id || libuuid.create();
        var log = options.log.child({
            req_id: id
        });

        dtrace['findobjects-start'].fire(function () {
            return ([res.msgid, id, b, f]);
        });
        res._num_records = 0;

        log.debug({
            bucket: b,
            filter: f,
            opts: opts
        }, 'find: entered');

        var filter;
        try {
            filter = ldap.parseFilter(f);
        } catch (e) {
            log.debug(e, 'bad search filter');
            res.end(new InvalidQueryError(e, f));
            return;
        }

        var _opts = {
            async: false,
            read: true
        };
        manatee.pg(_opts, function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'find: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'find: transaction started');

            vasync.pipeline({
                funcs: [
                    function begin(_, _cb) {
                        pg.begin('REPEATABLE READ', _cb);
                    },
                    common.loadBucket,
                    common.buildWhereClause,
                    getCount,
                    getRecords
                ],
                arg: {
                    bucket: {
                        name: b
                    },
                    filter: filter,
                    log: log,
                    pg: pg,
                    manatee: manatee,
                    opts: opts,
                    req_id: id,
                    res: res
                }
            }, function (err) {
                log.debug(err, 'find: %s',
                          err ? 'failed' : 'done');
                if (err) {
                    res.end(err);
                } else {
                    res.end();
                }
                dtrace['findobjects-done'].fire(function () {
                    return ([res.msgid, res._num_records]);
                });
                pg.commit(function () {});
            });

            return;
        });
    }

    return (_find);
}



///--- Exports

module.exports = {
    find: find
};
