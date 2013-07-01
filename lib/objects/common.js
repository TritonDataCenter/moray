// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var crc = require('crc');
var LRU = require('lru-cache');
var once = require('once');
var vasync = require('vasync');

var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;

var BCACHE = new LRU({
        name: 'BucketCache',
        max: 100,
        maxAge: (300 * 1000)
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
                        value = val;
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
                        value =  val;
                        break;
                default:
                        break;
                }
        }

        return (value);
}


function compileQuery(b, s, f, count) {
        assert.string(b, 'bucket');
        assert.object(s, 'schema');
        assert.object(f, 'query');

        count = count || 0;

        var args = [];
        var clause = '';
        var i;
        var type;
        var v;

        switch (f.type) {
        case 'and':
                var ands = [];
                f.filters.forEach(function (_f) {
                        v = compileQuery(b, s, _f, count);
                        if (v && v.clause.length > 0) {
                                ands.push(v);
                                args = args.concat(v.args);
                                count += v.args.length;
                        }
                });
                if (ands.length === 0)
                        throw new NotIndexedError(b, f.toString());

                type = f.type.toUpperCase();
                for (i = 0; i < ands.length; i++) {
                        clause += ' (' + ands[i].clause + ') ';
                        if (i < ands.length - 1)
                                clause += type;
                }
                break;

        case 'or':
                var ors = [];
                f.filters.forEach(function (_f) {
                        v = compileQuery(b, s, _f, count);
                        if (!v || !v.clause.length)
                                throw new NotIndexedError(b, f.toString());

                        ors.push(v);
                        args = args.concat(v.args);
                        count += v.args.length;
                });
                if (ors.length === 0)
                        throw new NotIndexedError(b, f.toString());

                type = f.type.toUpperCase();
                for (i = 0; i < ors.length; i++) {
                        clause += ' (' + ors[i].clause + ') ';
                        if (i < ors.length - 1)
                                clause += type;
                }
                break;

        case 'not':
                v = compileQuery(b, s, f.filter, count);
                if (v.clause.length > 0) {
                        args = args.concat(v.args);
                        clause += ' NOT (' + v.clause + ')';
                        count += v.args.length;
                }
                break;

        case 'ge':
                v = _value(s, f.attribute, f.value, f);
                if (v !== undefined) {
                        args.push(v);
                        clause += f.attribute + ' >= $' + (++count);
                }
                break;

        case 'le':
                v = _value(s, f.attribute, f.value, f);
                if (v !== undefined) {
                        args.push(v);
                        clause += f.attribute + ' <= $' + (++count);
                }
                break;

        case 'present':
                if (_value(s, f.attribute, ' ', f) !== undefined)
                        clause += f.attribute + ' IS NOT NULL';
                break;

        case 'substring':
                var _like_tmp = '';
                if (f.initial)
                        _like_tmp += _value(s, f.attribute, f.initial, f) + '%';

                f.any.forEach(function (_f) {
                        _like_tmp += '%' + _value(s, f.attribute, _f, f) + '%';
                });
                if (f['final']) {
                        _like_tmp += '%' + _value(s,
                                                  f.attribute,
                                                  f['final'],
                                                  f) + '%';
                }

                args.push(_like_tmp);
                clause += f.attribute + ' LIKE $' + (++count);

                break;

        case 'equal':
        default:
                v = _value(s, f.attribute, f.value, f);
                if (v !== undefined) {
                        args.push(v);
                        clause += f.attribute + ' = $' + (++count);
                }
                break;
        }

        if (count === undefined && clause.length === 0)
                throw new NotIndexedError(b, f.toString());

        return ({
                args: args,
                clause: clause,
                count: count
        });
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
                var q = compileQuery(b.name, b.index, f);
                where += q.clause;
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

                opts.where = {
                        clause: where,
                        args: q.args
                };
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
        var b = req.bucket;
        var etag = req.etag !== undefined ? req.etag : req._etag;
        var log = req.log;
        var k = req.key;
        var old = (req.previous || {})._etag;

        //
        // So the logic for etag checking below is as follows (assume 'etag' is
        // caller-specified value):
        //
        // - if the etag is 'undefined', no-op, caller doesn't care (common)
        // - if the etag is 'null', there must be no previous record
        // - otherwise, the etag has to match the existing record (if there was
        //   no existing record, that's an error)
        //

        if (etag === undefined) {
                log.debug('checkEtag: etag undefined');
        } else if (etag === null) {
                log.debug('checkEtag: etag null');
                if (old) {
                        cb(new EtagConflictError(b.name, k, 'null', old));
                        return;
                }
        } else if (etag !== old) {
                cb(new EtagConflictError(b.name, k, etag, old || 'null'));
                return;
        }

        cb();
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
                        pg.rollback();
                        res.end(pgError(err));
                        return;
                }

                pg.commit(function (err2) {
                        if (err2) {
                                log.debug(err2, '%s: failed', n);
                                res.end(pgError(err2));
                        } else {
                                log.debug('%s: done', n);
                                res.end({
                                        count: res._count,
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
        var sql = sprintf('SELECT *, \'%s\' AS req_id FROM buckets_config ' +
                          'WHERE name=$1', opts.req_id || 'null');

        log.debug({
                bucket: b
        }, 'loadBucket: entered');

        cb = once(cb);

        q = pg.query(sql, [b]);

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

                        var keys = Object.keys(opts.bucket.index || {});
                        opts.bucket._indexKeys = keys.map(function (k) {
                                return ({
                                        key: k,
                                        lcKey: k.toLowerCase()
                                });
                        });

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

        // Moray supports 'update', which updates the PG columns, but
        // not the serialized JSON.  Here, we do the following:
        // - if the PG column is null, delete the returned value
        // - if the PG column has a value:
        //   - if the serialized data is either not there or the serialized
        //     form is there and is not an array, blindly overwrite
        // - If the serialied data is present and an Array, we just skip - note
        //   the only system using this is UFDS, and indexes with arrays (really
        //   multi-valued attributes) is already goofy and not-supported, so we
        //   can safely ignore it, generally speaking.
        bucket._indexKeys.forEach(function (key) {
                var k = key.key;
                var v = row[key.lcKey];
                if (v === undefined || v === null) {
                        if (obj.value[k])
                                delete obj.value[k];
                } else {
                        if (!obj.value[k] || !Array.isArray(obj.value[k])) {
                                obj.value[k] = v;
                        }
                }
        });

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
        var sql = sprintf('SELECT *, \'%s\' AS req_id FROM %s WHERE ' +
                          '_key=$1 FOR UPDATE', opts.req_id || 'null', bucket);

        log.debug({
                bucket: bucket,
                key: key
        }, 'selectForUpdate: entered');
        q = pg.query(sql, [key]);

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
