// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var ldap = require('ldapjs');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers

function getRecords(req, cb) {
        var bucket = req.bucket.name;
        var filter = req.filter;
        var log = req.log;
        var res = req.res;
        var sql = sprintf('SELECT *, COUNT(1) over () AS _count, ' +
                          '\'%s\' AS req_id FROM %s %s',
                          req.req_id, bucket, req.where);

        log.debug({
                bucket: req.bucket.name,
                key: req.key,
                sql: sql
        }, 'getRecords: entered');

        var query = req.pg.query(sql);
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
                v._count = obj._count;

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
                var id = opts.req_id || uuid.v1();
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

                var _opts = {
                        async: false,
                        read: true
                };
                manatee.start(_opts, function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'find: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'find: transaction started');

                        var filter;
                        try {
                                filter = ldap.parseFilter(f);
                        } catch (e) {
                                log.debug(e, 'bad search filter');
                                e = new InvalidQueryError(e, f);
                                res.end(e);
                                return;
                        }

                        vasync.pipeline({
                                funcs: [
                                        common.loadBucket,
                                        common.buildWhereClause,
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
                                pg.rollback();
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
