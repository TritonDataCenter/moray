// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var Cache = require('expiring-lru-cache');
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
        updateRows
];



///--- Handlers

function updateRows(req, cb) {
        var b = req.bucket;
        var etag = 'u' + uuid.v1().substr(0, 7);
        var ok = true;
        var column = '_etag=$1,_mtime=$2';
        var i = 3;
        var log = req.log;
        var pg = req.pg;
        var q;
        var sql;
        var vals = [etag + '', Date.now()];

        cb = once(cb);

        req.fieldKeys.forEach(function (k) {
                if (!req.bucket.index[k]) {
                        ok = false;
                        return;
                }

                column += ',' + k + '=$' + (i++);
                switch (req.bucket.index[k].type) {
                case 'boolean':
                        vals.push(req.fields[k].toUpperCase());
                        break;
                case 'number':
                        vals.push(parseInt(req.fields[k], 10));
                        break;
                case 'string':
                        vals.push(req.fields[k] + '');
                        break;
                default:
                        break;
                }
        });

        if (!ok) {
                cb(new NotIndexedError(b.name, JSON.stringify(req.fields)));
                return;
        }

        sql = sprintf('UPDATE %s SET %s %s RETURNING \'%s\' AS req_id',
                      b.name, column, req.where, req.req_id);

        req.log.debug({
                bucket: req.bucket.name,
                sql: sql,
                vals: vals
        }, 'updateRows: entered');

        q = pg.query(sql, vals);
        q.once('error', function (err) {
                log.debug(err, 'updateRows: failed');
                cb(err);
        });
        q.once('end', function () {
                req.res._etag = etag;
                log.debug({etag: req.res._etag}, 'updateRows: done');
                cb();
        });
}


function update(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _update(b, fields, f, opts, res) {
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
                var keys = Object.keys(fields);
                var log = options.log.child({
                        req_id: id
                });

                log.debug({
                        bucket: b,
                        fields: fields,
                        filter: filter,
                        opts: opts
                }, 'update: entered');

                if (keys.length === 0) {
                        res.end(new FieldUpdateError(fields));
                        return;
                }

                dtrace['update-start'].fire(function () {
                        return ([res.msgid, id, b, fields, f]);
                });

                opts.noLimit = true;
                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'update: no DB handle');
                                res.end(startErr);
                                return;
                        }

                        log.debug({
                                pg: pg
                        }, 'update: transaction started');

                        vasync.pipeline({
                                funcs: PIPELINE,
                                arg: {
                                        bucket: {
                                                name: b
                                        },
                                        fields: fields,
                                        fieldKeys: keys,
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
                                name: 'update',
                                pg: pg,
                                res: res
                        }));
                });
        }

        return (_update);
}



///--- Exports

module.exports = {
        update: update,
        pipeline: PIPELINE
};
