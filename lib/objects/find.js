// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var ldap = require('ldapjs');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- Internal Functions


function _value(schema, key, val, filter) {
        var value;

        if (schema[key]) {
                switch (schema[key].type) {
                case 'boolean':
                        filter.value = value = (val ? 'true' : 'false');
                        break;

                case 'number':
                        filter.value = value = parseInt(val, 10);
                        break;

                default:
                        value =  '\'' + val + '\'';
                        break;
                }
        } else if (/^_\w+/.test(key)) {
                switch (key) {
                case '_id':
                case '_txn_snap':
                case '_mtime':
                        filter.value = value = parseInt(val, 10);
                        if (isNaN(value))
                                value = ' ';
                        break;
                case '_etag':
                        value =  '\'' + val + '\'';
                        break;
                default:
                        break;
                }
        }

        return (value);
}


function _compileQuery(bucket, schema, query, child) {
        assert.string(bucket, 'bucket');
        assert.object(schema, 'schema');
        assert.object(query, 'query');

        var _f;
        var _t;
        var f = query;
        var i;
        var where = '';
        var val;
        var vals;

        switch (f.type) {
        case 'and':
                vals = [];
                for (i = 0; i < f.filters.length; i++) {
                        _f = f.filters[i];
                        val = _compileQuery(bucket, schema, _f, true);
                        if (val.length > 0) {
                                vals.push(val);
                        }
                }
                if (vals.length === 0)
                        throw new NotIndexedError(bucket, query.toString());

                _t = f.type.toUpperCase();
                where += '(' + vals.join(' ' + _t + ' ') + ')';
                break;

        case 'or':
                vals = [];
                for (i = 0; i < f.filters.length; i++) {
                        _f = f.filters[i];
                        val = _compileQuery(bucket, schema, _f, true);
                        if (!val) {
                                throw new NotIndexedError(bucket,
                                                          query.toString());
                        }
                        vals.push(val);
                }

                _t = f.type.toUpperCase();
                where += '(' + vals.join(' ' + _t + ' ') + ')';
                break;

        case 'not':
                val = _compileQuery(bucket, schema, f.filter, true);
                if (val.length > 0) {
                        where += ' (NOT (' + val + '))';
                }
                break;

        case 'ge':
                val = _value(schema, f.attribute, f.value, f);
                if (val !== undefined)
                        where += f.attribute + ' >= ' + val;
                break;

        case 'le':
                val = _value(schema, f.attribute, f.value, f);
                if (val !== undefined)
                        where += f.attribute + ' <= ' + val;
                break;

        case 'present':
                if (_value(schema, f.attribute, ' ', f) !== undefined)
                        where += f.attribute + ' IS NOT NULL';
                break;

        case 'substring':
                val = _value(schema, f.attribute, f.value, f);
                if (val !== undefined) {
                        where += f.attribute + ' LIKE \'';
                        if (f.initial)
                                where += f.initial + '%';
                        f.any.forEach(function (s) {
                                where += '%' + s + '%';
                        });
                        if (f['final'])
                                where += '%' + f['final'];

                        where += '\'';
                }
                break;

        case 'equal':
        default:
                val = _value(schema, f.attribute, f.value, f);
                if (val !== undefined) {
                        where += f.attribute + ' = ' + val;
                }
                break;
        }

        if (!child && where.length === 0) {
                throw new NotIndexedError(bucket, query.toString());
        }

        return (where);
}



///--- Handlers

function buildWhereClause(req, cb) {
        var b = req.bucket;
        var where;

        try {
                where = _compileQuery(b.name, b.index, req.filter);
                if (req.opts.sort && req.opts.sort.attribute) {
                        where += ' ORDER BY ' + req.opts.sort.attribute;
                        if (req.opts.sort.order) {
                                where += ' ' + req.opts.sort.order;
                        }
                }
                where += ' LIMIT '+ (req.opts.limit || 1000);
                if (req.opts.offset)
                        where += ' OFFSET ' + req.opts.offset;

                req.where = where;
        } catch (e) {
                req.log.debug(e, 'buildWhereClause: failed');
                return (cb(e));
        }
        if (!req.where) {
                req.log.debug('Unable to generate WHERE clause');
                return (cb(new InvalidQueryError(req.filter.toString())));
        }
        return (cb());
}


function getRecords(req, cb) {
        var bucket = req.bucket.name;
        var filter = req.filter;
        var log = req.log;
        var res = req.res;
        var sql = sprintf('SELECT _id, _key, _value, _etag, _mtime, ' +
                          '_txn_snap, COUNT(1) over () AS _count FROM %s ' +
                          'WHERE %s',
                          bucket, req.where);

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
                var obj = common.rowToObject(bucket, row);

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
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

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
                                res.end(e);
                                return;
                        }

                        vasync.pipeline({
                                funcs: [
                                        common.loadBucket,
                                        buildWhereClause,
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
