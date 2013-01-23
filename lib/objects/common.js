// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var crc = require('crc');
var Cache = require('expiring-lru-cache');
var vasync = require('vasync');

var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;

var BCACHE = new Cache({
        name: 'BucketCache',
        size: 100,
        expiry: (300 * 1000)
});



///--- Internal Helpers

function _value(schema, key, val, filter) {
        var value;

        if (schema[key]) {
                switch (schema[key].type) {
                case 'boolean':
                        filter.value = /^true$/i.test(val);
                        value = filter.value.toString();
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


function compileQuery(bucket, schema, query, child) {
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
                        val = compileQuery(bucket, schema, _f, true);
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
                        val = compileQuery(bucket, schema, _f, true);
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
                val = compileQuery(bucket, schema, f.filter, true);
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


///--- API

function buildWhereClause(opts, cb) {
        assert.object(opts, 'options');
        assert.object(opts.bucket, 'options.bucket');
        assert.object(opts.filter, 'options.filter');
        assert.object(opts.log, 'options.log');
        assert.object(opts.opts, 'options.opts');
        assert.func(cb, 'callback');

        var b = opts.bucket;
        var f = opts.filter;
        var log = opts.log;
        var o = opts.opts;
        var where = 'WHERE ';

        try {
                where += compileQuery(b.name, b.index, f);
                if (o.sort && o.sort.attribute) {
                        where += ' ORDER BY ' + o.sort.attribute;
                        if (o.sort.order) {
                                where += ' ' + o.sort.order;
                        }
                }
                if (!o.noLimit)
                        where += ' LIMIT '+ (o.limit || 1000);
                if (o.offset)
                        where += ' OFFSET ' + o.offset;

                opts.where = where;
        } catch (e) {
                log.debug(e, 'buildWhereClause: failed');
                cb(e);
                return;
        }

        if (!opts.where) {
                log.debug('Unable to generate WHERE clause');
                cb(new InvalidQueryError(f.toString()));
                return;
        }

        cb();
}


function cacheKey(b, k) {
        assert.string(b, 'bucket');

        var str = '/' + b;
        if (k)
                str += '/' + k;

        return (str);
}


function checkEtag(req, cb) {
        var etag = req.etag || req._etag;
        if (!req.previous || !etag)
                return (cb());

        if (req.previous._etag !== etag)
                return (cb(new EtagConflictError(req.bucket.name,
                                                 req.key,
                                                 etag,
                                                 req.previous._etag)));

        return (cb());
}


function pipelineCallback(opts) {
        assert.object(opts, 'options');
        assert.string(opts.name, 'options.name');
        assert.object(opts.log, 'options.log');
        assert.object(opts.pg, 'options.pg');
        assert.object(opts.res, 'options.res');

        var n = opts.name;
        var log = opts.log;
        var pg = opts.pg;
        var res = opts.res;

        function _callback(err) {
                if (err) {
                        log.debug(err, '%s: failed', n);
                        res.end(pgError(err));
                        pg.rollback();
                        return;
                }

                pg.commit(function (err2) {
                        if (err2) {
                                log.debug(err2, '%s: failed', n);
                                res.end(pgError(err2));
                        } else {
                                log.debug('%s: done', n);
                                res.end({
                                        etag: res._etag
                                });
                        }

                        var probe = n.toLowerCase() + '-done';
                        if (dtrace[probe]) {
                                dtrace[probe].fire(function () {
                                        return ([opts.res.msgid]);
                                });
                        }
                });

                return;
        }

        return (_callback);
}


function createEtag(opts, cb) {
        opts.etag = crc.hex32(crc.crc32(opts._value));
        cb();
}


function loadBucket(opts, cb) {
        assert.object(opts, 'options');
        assert.object(opts.bucket, 'options.bucket');
        assert.object(opts.log, 'options.log');
        assert.object(opts.pg, 'options.pg');
        assert.func(cb, 'callback');

        var b;
        var ckey = cacheKey(opts.bucket.name);
        var log = opts.log;
        if ((b = BCACHE.get(ckey))) {
                opts.bucket = b;
                log.debug({
                        bucket: b
                }, 'loadBucket: done (cached)');
                cb(null);
                return;
        }

        b = opts.bucket.name;
        var pg = opts.pg;
        var q;
        var row;
        var sql = sprintf('SELECT * FROM buckets_config WHERE name=\'%s\'', b);

        log.debug({
                bucket: b
        }, 'loadBucket: entered');

        q = pg.query(sql);

        q.once('error', function (err) {
                log.debug({
                        bucket: b,
                        err: err
                }, 'loadBucket: failed');
                cb(err);
        });

        q.once('row', function (r) {
                row = r;
        });

        q.once('end', function (result) {
                if (!row) {
                        cb(new BucketNotFoundError(opts.bucket.name));
                } else {
                        function parseFunctor(f) {
                                var fn;
                                assert.ok(eval('fn = ' + f));
                                return (fn);
                        }

                        var r = row;
                        opts.bucket = {
                                name: r.name,
                                index: JSON.parse(r.index),
                                pre: JSON.parse(r.pre).map(parseFunctor),
                                post: JSON.parse(r.post).map(parseFunctor),
                                options: JSON.parse(r.options || {}),
                                mtime: new Date(r.mtime)
                        };

                        opts.bucket._indexKeys =
                                Object.keys(opts.bucket.index || {});

                        BCACHE.set(ckey, opts.bucket);
                        log.debug({
                                bucket: opts.bucket
                        }, 'loadBucket: done');
                        cb(null);
                }
        });
}


function pgError(e) {
        var err;
        var msg;

        switch (e.code) {
        case '23505':
                /* JSSTYLED */
                msg = /.*violates unique constraint.*/.test(e.message);
                if (msg) {
                        // Key (str_u)=(hi) already exists
                        /* JSSTYLED */
                        msg = /.*\((.*)\)=\((.*)\)/.exec(e.detail);
                        err = new UniqueAttributeError(err, msg[1], msg[2]);
                } else {
                        err = e;
                }
                break;
        case '42601':
        case '42701':
                err = new InternalError(e, 'Invalid SQL: %s', e.message);
                break;
        default:
                err = e;
                break;
        }

        return (err);
}

function rowToObject(bucket, row) {
        assert.object(bucket, 'bucket');
        assert.object(row, 'row');

        var obj = {
                bucket: bucket.name,
                key: row._key,
                value: JSON.parse(row._value),
                _id: row._id,
                _etag: row._etag,
                _mtime: row._mtime,
                _txn_snap: row._txn_snap,
                _count: row._count
        };

        if ((bucket.options || {}).syncUpdates) {
                bucket._indexKeys.forEach(function (k) {
                        if (row[k] !== undefined)
                                obj.value[k] = row[k];
                });
        }

        return (obj);
}


function runPostChain(opts, cb) {
        if (opts.bucket.post.length === 0)
                return (cb());

        var cookie = {
                bucket: opts.bucket.name,
                id: opts.value ? opts.value._id : (opts._id || -1),
                key: opts.key,
                log: opts.log,
                pg: opts.pg,
                schema: opts.bucket.index,
                value: opts.value,
                headers: opts.headers || {}
        };
        var log = opts.log;

        log.debug('runPostChain: entered');

        vasync.pipeline({
                funcs: opts.bucket.post,
                arg: cookie
        }, function (err) {
                if (err) {
                        log.debug(err, 'runPostChain: fail');
                        cb(err);
                } else {
                        log.debug('runPostChain: done');
                        cb();
                }
        });

        return (undefined);
}


function selectForUpdate(opts, cb) {
        var bucket = opts.bucket.name;
        var key = opts.key;
        var log = opts.log;
        var pg = opts.pg;
        var q;
        var row;
        var sql = sprintf('SELECT * FROM %s WHERE _key=\'%s\' FOR UPDATE',
                          bucket, key);

        log.debug({
                bucket: bucket,
                key: key
        }, 'selectForUpdate: entered');
        q = pg.query(sql);

        q.once('error', function (err) {
                log.debug(err, 'selectForUpdate: failed');
                cb(err);
        });

        q.once('row', function (r) {
                row = r;
        });

        q.once('end', function (result) {
                if (row)
                        opts.previous = row;

                log.debug({
                        previous: opts.previous || null
                }, 'selectForUpdate: done');
                cb();
        });
}



///--- Exports

module.exports = {
        buildWhereClause: buildWhereClause,
        cacheKey: cacheKey,
        checkEtag: checkEtag,
        loadBucket: loadBucket,
        pgError: pgError,
        pipelineCallback: pipelineCallback,
        rowToObject: rowToObject,
        runPostChain: runPostChain,
        selectForUpdate: selectForUpdate
};
