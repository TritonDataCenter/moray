// Copyright 2013 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var microtime = require('microtime');
var crc = require('crc');
var ldap = require('ldapjs');
var once = require('once');
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
        common.buildWhereClause,
        drop
];



///--- Handlers

function drop(req, cb) {
        cb = once(cb);

        var b = req.bucket.name;
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql = sprintf('DELETE FROM %s %s', b, req.where);

        log.debug({
                bucket: req.bucket.name,
                sql: sql
        }, 'drop: entered');

        q = pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'drop: failed');
                cb(err);
        });

        q.once('end', function () {
                log.debug({id: req._id}, 'drop: done');
                cb();
        });
}


function deleteMany(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _deleteMany(b, f, opts, res) {
                var filter;
                try {
                        filter = ldap.parseFilter(f);
                } catch (e) {
                        log.debug(e, 'bad search filter');
                        e = new InvalidQueryError(e, f);
                        res.end(e);
                        return;
                }

                var id = opts.req_id || uuid.v1();
                var log = options.log.child({
                        req_id: id
                });

                log.debug({
                        bucket: b,
                        filter: filter,
                        opts: opts
                }, 'deleteMany: entered');

                dtrace['delmany-start'].fire(function () {
                        return ([res.msgid, id, b, f]);
                });

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'deleteMany: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'deleteMany: transaction started');

                        opts.noLimit = true;
                        vasync.pipeline({
                                funcs: PIPELINE,
                                arg: {
                                        bucket: {
                                                name: b
                                        },
                                        filter: filter,
                                        log: log,
                                        pg: pg,
                                        opts: opts,
                                        manatee: manatee,
                                        req_id: id,
                                        res: res
                                }
                        }, common.pipelineCallback({
                                log: log,
                                name: 'delmany',
                                pg: pg,
                                res: res
                        }));
                });
        }

        return (_deleteMany);
}



///--- Exports

module.exports = {
        deleteMany: deleteMany,
        pipeline: PIPELINE
};
