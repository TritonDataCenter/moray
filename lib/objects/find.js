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


function _value(schema, key, val) {
        var value;

        if (schema[key]) {
                switch (schema[key].type) {
                case 'boolean':
                        value = val ? 'true' : 'false';
                        break;

                case 'number':
                        value = parseInt(val, 10);
                        break;

                default:
                        value =  '\'' + val + '\'';
                        break;
                }
        } else if (/^_\w+/.test(key)) {
                switch (key) {
                case '_id':
                case '_mtime':
                        value = parseInt(val, 10);
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
        assert.string(query, 'query');

        var f = ldap.parseFilter(query);
        var where = '';
        var val;
        var vals;

        switch (f.type) {
        case 'and':
        case 'or':
                vals = [];
                for (var i = 0; i < f.filters.length; i++) {
                        var _f = f.filters[i].toString();
                        val = _compileQuery(bucket, schema, _f, true);
                        if (val.length > 0) {
                                vals.push(val);
                        }
                }
                if (vals.length === 0)
                        throw new NotIndexedError(bucket, query);

                var _t = f.type.toUpperCase();
                where += '(' + vals.join(' ' + _t + ' ') + ')';
                break;

        case 'not':
                val = _compileQuery(bucket, schema, f.filter.toString(), true);
                if (val.length > 0) {
                        where += ' (NOT (' + val + '))';
                }
                break;

        case 'ge':
                if ((val = _value(schema, f.attribute, f.value)) !== undefined)
                        where += f.attribute + ' >= ' + val;
                break;

        case 'le':
                if ((val = _value(schema, f.attribute, f.value)) !== undefined)
                        where += f.attribute + ' <= ' + val;
                break;

        case 'present':
                if (_value(schema, f.attribute, ' ') !== undefined)
                        where += f.attribute + ' IS NOT NULL';
                break;

        case 'substring':
                if ((val = _value(schema, f.attribute, ' ')) !== undefined) {
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
                val = _value(schema, f.attribute, f.value);
                if (val !== undefined) {
                        where += f.attribute + ' = ' + val;
                }
                break;
        }

        if (!child && where.length === 0) {
                throw new NotIndexedError(bucket, query);
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
                return (cb(new InvalidQueryError(req.filter)));
        }
        return (cb());
}


function getRecords(req, cb) {
        var bucket = req.bucket.name;
        var log = req.log;
        var res = req.res;
        var sql = sprintf('SELECT _id, _key, _value, _etag, _mtime ' +
                          'FROM %s WHERE %s', bucket, req.where);

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
                res.write(common.rowToObject(bucket, row));
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

                manatee.start({read: true}, function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr, 'find: no DB handle');
                                return (res.end(startErr));
                        }

                        log.debug({
                                pg: pg
                        }, 'find: transaction started');

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
                                        filter: f,
                                        log: log,
                                        pg: pg,
                                        manatee: manatee,
                                        opts: opts,
                                        res: res
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'find: failed');
                                        res.end(err);
                                } else {
                                        log.debug('find: done');
                                        res.end();
                                }
                                pg.rollback(function () {});
                        });

                        return (undefined);
                });
        }

        return (_find);
}



///--- Exports

module.exports = {
        find: find
};
