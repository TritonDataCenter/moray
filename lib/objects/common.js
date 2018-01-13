/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var util = require('util');
var net = require('net');

var assert = require('assert-plus');
var filters = require('moray-filter');
var ip6addr = require('ip6addr');
var LRU = require('lru-cache');
var once = require('once');
var vasync = require('vasync');
var clone = require('clone');
var vm = require('vm');

var control = require('../control');
var dtrace = require('../dtrace');

var mod_errors = require('../errors');
var BucketNotFoundError = mod_errors.BucketNotFoundError;
var NotIndexedError = mod_errors.NotIndexedError;
var InvalidIndexTypeError = mod_errors.InvalidIndexTypeError;
var InvalidRequireError = mod_errors.InvalidRequireError;
var InvalidQueryError = mod_errors.InvalidQueryError;
var EtagConflictError = mod_errors.EtagConflictError;

var pgCommon = require('../pg');
var pgError = pgCommon.pgError;
var typeToPg = pgCommon.typeToPg;

var TYPES = require('../types').TYPES;


///--- Globals

var INTERNAL_FIELDS = ['_etag', '_key', '_id', '_mtime', '_txn_snap'];


///--- Internal Helpers

/**
 * This function is used during processing of the parsed Moray filter to:
 *
 *   - Convert the string value into what will be sent to Postgres as a
 *     parameter to the generated SQL query.
 *   - Update the filter object with the JavaScript value to compare to.
 *   - Determine if this is an array type.
 */
function _value(schema, key, val, filter) {
    var array = false;
    var value;

    if (schema[key]) {
        switch (schema[key].type) {
        case 'boolean':
            filter.value = /^true$/i.test(val);
            value = filter.value.toString().toUpperCase();
            break;

        case '[boolean]':
            filter.value = /^true$/i.test(val);
            value = filter.value.toString().toUpperCase();
            array = true;
            break;

        case 'number':
            filter.value = value = parseInt(val, 10);
            break;

        case '[number]':
            filter.value = value = parseInt(val, 10);
            array = true;
            break;

        case 'ip':
            filter.value = ip6addr.parse(val);
            value = filter.value.toString();
            break;

        case '[ip]':
            filter.value = ip6addr.parse(val);
            value = filter.value.toString();
            array = true;
            break;

        case 'subnet':
            filter.value = ip6addr.createCIDR(val);
            value = filter.value.toString();
            break;

        case '[subnet]':
            filter.value = ip6addr.createCIDR(val);
            value = filter.value.toString();
            array = true;
            break;

        case '[string]':
            value = val;
            array = true;
            break;

        default:
            value = val;
            break;
        }
    } else {
        switch (key) {
        case '_id':
        case '_txn_snap':
        case '_mtime':
            filter.value = value = parseInt(val, 10);
            if (isNaN(value))
                value = ' ';
            break;
        case '_etag':
        case '_key':
            value =  val;
            break;
        default:
            break;
        }
    }

    return ({
        isArray: array,
        value: value
    });
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

    function _append(op) {
        v = _value(s, f.attribute, f.value, f);
        if (v.value !== undefined) {
            args.push(v.value);
            count += 1;
            clause += ' ( ';
            if (v.isArray) {
                if (op === '=') {
                    clause += f.attribute + ' @> ARRAY[$' + count + ']::'
                        + typeToPg(s[f.attribute].type);
                } else {
                    clause += '$' + count + ' ' + op +
                        ' ANY(' + f.attribute + ')';
                }
            } else {
                clause += f.attribute + ' ' + op + ' $' + count;
                clause += ' AND ' + f.attribute + ' IS NOT NULL';
            }
            clause += ' ) ';
        }
    }

    function _substr(_f, op) {
        op = op || 'LIKE';
        var _like_tmp = '';
        var _v;
        var valid = true;

        if (_f.initial) {
            _v = _value(s, _f.attribute, _f.initial, _f);
            if (_v.isArray)
                throw new NotIndexedError(b, _f.toString());
            if (_v.value === undefined)
                valid = false;
            _like_tmp += _v.value + '%';
        }

        _f.any.forEach(function (__f) {
            _v = _value(s, _f.attribute, __f, _f);
            if (_v.isArray)
                throw new NotIndexedError(b, _f.toString());
            if (_v.value === undefined)
                valid = false;
            _like_tmp += '%' + _v.value + '%';
        });
        if (_f['final']) {
            _v = _value(s, f.attribute, _f['final'], _f);
            if (_v.isArray)
                throw new NotIndexedError(b, _f.toString());
            if (_v.value === undefined)
                valid = false;
            _like_tmp += '%' + _v.value + '%';
        }

        if (valid) {
            args.push(_like_tmp);
            clause += ' ( ';
            clause += _f.attribute + ' ' + op + ' $' + (++count);
            clause += ' AND ' + _f.attribute + ' IS NOT NULL';
            clause += ' ) ';
        }
    }

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

    case 'substring':
        if (!s[f.attribute] && !/^_\w+/.test(f.attribute))
            break;

        _substr(f, 'LIKE');
        break;

    case 'present':
        if (s[f.attribute])
            clause += f.attribute + ' IS NOT NULL';
        break;

    case 'ge':
        _append('>=');
        break;

    case 'le':
        _append('<=');
        break;

    case 'ext':
        switch (f.rule) {
        case 'caseIgnoreMatch':
            _append('ILIKE');
            break;

        case 'caseIgnoreSubstringsMatch':
            _substr(f, 'ILIKE');
            break;

        default:
            throw new NotIndexedError(b, f.toString());
        }
        break;

    case 'equal':
    default:
        _append('=');
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


/**
 * Maps a value from the JSON object sent by the client into a value to insert
 * into a Postgres column.
 */
function _mapType(type, value, key, isArray) {
    var ret;
    switch (type) {
    case 'boolean':
        ret = value.toString().toUpperCase();
        break;

    case 'number':
        ret = parseInt(value, 10);
        break;

    case 'string':
        ret = value + '';
        // Strings in arrays require extra escaping precaution
        /* JSSTYLED */
        if (isArray && /[",{}\\]/.test(ret)) {
            /* JSSTYLED */
            ret = '"' + ret.replace(/([",{}\\])/g, '\\$1') + '"';
        }
        break;

    case 'ip':
        try {
            ret = ip6addr.parse(value).toString();
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'subnet':
        ret = value + '';
        break;

    default:
        throw new InvalidIndexTypeError(key, type);
    }
    return (ret);
}


/**
 * Decorate ext filter with CaseInsensitiveMatch attributes/methods.
 */
function _matchCaseInsensitive(filter) {
    function matches(target) {
        var tv = filters.getAttrValue(target, this.matchType);
        var value = this.value.toLowerCase();
        return filters.testValues(function (v) {
            if (typeof (v) === 'string') {
                return value === v.toLowerCase();
            } else {
                return false;
            }
        }, tv);
    }
    filter.matches = matches.bind(filter);
}

/**
 * Decorate ext filter with CaseInsensitiveSubstrMatch attributes/methods.
 */
function _matchCaseInsensitiveSubstr(filter) {
    var f = filters.parse(util.format('(%s=%s)',
                filter.attribute, filter.value.toLowerCase()));

    // extract substr fields to build SQL statement
    filter.initial = f.initial;
    filter.any = f.any;
    filter.final = f.final;

    function matches(target) {
        var attr = this.attribute;
        var tv = filters.getAttrValue(target, attr);

        return filters.testValues(function (v) {
            if (typeof (v) === 'string') {
                var obj = {};
                obj[attr] = v.toLowerCase();
                return f.matches(obj);
            } else {
                return false;
            }
        }, tv);
    }
    filter.matches = matches.bind(filter);
}

/**
 * Decorate ge/le filters to support ip type.
 */
function _matchTypeIP(filter) {
    function matchesIP(target) {
        var self = this;
        var tv = filters.getAttrValue(target, this.attribute);

        return filters.testValues(function (v) {
            try {
                switch (self.type) {
                case 'ge':
                    return (ip6addr.compare(v, self.value) >= 0);
                case 'le':
                    return (ip6addr.compare(v, self.value) <= 0);
                case 'equal':
                    return (ip6addr.compare(v, self.value) === 0);
                default:
                    return false;
                }
            } catch (e) {
                return false;
            }
        }, tv);
    }
    if (filter.type === 'ge' ||
        filter.type === 'le' ||
        filter.type === 'equal') {
        filter.value = ip6addr.parse(filter.value);
        filter.matches = matchesIP.bind(filter);
    }
}

/**
 * Decorate ge/le filters to support subnet type.
 */
function _matchTypeSubnet(filter) {
    function matchesSubnet(target) {
        var self = this;
        var tv = filters.getAttrValue(target, self.attribute);

        return filters.testValues(function (v) {
            try {
                switch (self.type) {
                case 'ge':
                    return (ip6addr.compareCIDR(v, self.value) >= 0);
                case 'le':
                    return (ip6addr.compareCIDR(v, self.value) <= 0);
                case 'equal':
                    return (ip6addr.compareCIDR(v, self.value) === 0);
                default:
                    return false;
                }
            } catch (e) {
                return false;
            }
        }, tv);
    }
    if (filter.type === 'ge' ||
        filter.type === 'le' ||
        filter.type === 'equal') {
        filter.matches = matchesSubnet.bind(filter);
    }
}


///--- API

function parseFilter(req, cb) {
    try {
        req.filter = filters.parse(req.rawFilter);
    } catch (e) {
        req.log.debug(e, 'bad search filter');
        cb(new InvalidQueryError(e, req.rawFilter));
        return;
    }
    cb();
}


function decorateFilter(req, cb) {
    assert.object(req, 'req');
    assert.object(req.bucket, 'req.bucket');
    assert.object(req.filter, 'req.filter');
    assert.func(cb, 'callback');

    req.idxBucket = req.bucket;
    if (req.bucket.reindex_active) {
        /*
         * Bucket columns which are presently being reindexed are assumed to
         * contain invalid data.  They are excluded from `req.idxBucket` so
         * they are not referenced when constructing a WHERE clause.
         */
        var exclude = [];
        Object.keys(req.bucket.reindex_active).forEach(function (key) {
            exclude = exclude.concat(req.bucket.reindex_active[key]);
        });
        if (exclude.length > 0) {
            var b = clone(req.bucket);
            exclude.forEach(function (field) {
                if (b.index[field]) {
                    delete b.index[field];
                }
            });
            req.idxBucket = b;
        }
    }

    try {
        var unindexed = [];
        req.filter.forEach(function (f) {
            if (f.type === 'approx') {
                throw new Error('approx filters not allowed');
            }
            if (f.type === 'ext') {
                switch (f.rule) {
                case '2.5.13.2':
                case 'caseIgnoreMatch':
                    _matchCaseInsensitive(f);
                    break;
                case '2.5.13.4':
                case 'caseIgnoreSubstringsMatch':
                    _matchCaseInsensitiveSubstr(f);
                    break;
                default:
                    throw new Error('unsupported ext filter');
                }
            }

            /* Support correct eq/ge/le comparison for special types */
            if (req.idxBucket.index[f.attribute] !== undefined) {
                switch (req.idxBucket.index[f.attribute].type) {
                case 'ip':
                case '[ip]':
                    _matchTypeIP(f);
                    break;
                case 'subnet':
                case '[subnet]':
                    _matchTypeSubnet(f);
                    break;
                default:
                    break;
                }
            }
            /*
             * Track filter attributes which refer to columns which are not
             * indexed or possess an invalid index
             */
            if (f.attribute !== undefined &&
                req.idxBucket.index[f.attribute] === undefined &&
                INTERNAL_FIELDS.indexOf(f.attribute) === -1) {
                unindexed.push(f.attribute);
            }
        });

        /*
         * If the filter expression refers _only_ to indexed fields, there is
         * little reason to re-check the filters locally.  That said, there may
         * be some behavior which depends on differences between how postgres
         * and moray perform filtering.  The redundant matching cannot be
         * skipped until it's confirmed that no SDC/Manta components rely on
         * such edge cases.
         */
    } catch (e) {
        req.log.debug(e, 'unable to decorate filter');
        cb(e);
        return;
    }
    cb();
}


function buildWhereClause(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.bucket, 'options.bucket');
    assert.object(opts.filter, 'options.filter');
    assert.object(opts.log, 'options.log');
    assert.object(opts.opts, 'options.opts');
    assert.func(cb, 'callback');

    var f = opts.filter;
    var log = opts.log;
    var o = opts.opts;
    var where = 'WHERE ';

    // Query only against fields with valid indices
    var b = opts.idxBucket;

    try {
        var q = compileQuery(b.name, b.index, f);
        if (!q.clause)
            throw new InvalidQueryError(f.toString());

        where += q.clause;
        if (o.sort) {
            if (Array.isArray(o.sort) && o.sort.length > 0) {
                var sort = '';
                o.sort.forEach(function (item) {
                    if (item.attribute) {
                        if (sort.length > 0) {
                            sort += ', ';
                        }
                        sort += item.attribute;
                        if (item.order) {
                            sort += ' ' + item.order;
                        }
                    }
                });
                if (sort.length > 0) {
                    where += ' ORDER BY ' + sort;
                }
            } else if (o.sort.attribute) {
                where += ' ORDER BY ' + o.sort.attribute;
                if (o.sort.order) {
                    where += ' ' + o.sort.order;
                }
            }
        }

        if (o.limit) {
            where += ' LIMIT ' + o.limit;
        } else if (!o.noLimit) {
            where += ' LIMIT ' + 1000;
        }
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
    var etag = req.opts.etag !== undefined ? req.opts.etag : req._etag;
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


function stdOutput(req) {
    // Default output (used by all but batch and findObjects)
    return (function () {
        return { count: req._count, etag: req._etag };
    });
}


/*
 * Some Moray triggers used the same "microtime" and "crc" modules that
 * Moray used. We allow those triggers to continue working by passing
 * our own require() function into their environment and restricting them
 * to only loading those modules.
 */
function guardedRequire(name) {
    if (name !== 'microtime' && name !== 'crc') {
        throw new InvalidRequireError(name);
    }

    return require(name);
}


function evalTrigger(f) {
    /*
     * We evaluate triggers in their own environment, to avoid leaking
     * functions and variables from our own. Several things are passed
     * into the environment of the evaluated triggers:
     *
     * - Error, so that all returned errors will be 'instanceof Error'
     * - require, to allow backwards-compatibility for triggers that
     *   used Moray's modules.
     */
    var ctx = {
        Error: Error,
        require: guardedRequire
    };

    vm.runInNewContext('fn = ' + f, ctx, 'moray-trigger');

    assert.func(ctx.fn, 'trigger');

    /*
     * We store the trigger in an object so that when it gets called
     * its 'this' value points to our object instead of to the scope
     * in which it was called.
     */
    var trigger = { run: ctx.fn };

    function safetyNet(arg, cb) {
        trigger.run(arg, once(cb));
    }

    return (safetyNet);
}


function loadBucket(req, cb) {
    assert.object(req, 'req');
    assert.object(req.bucket, 'req.bucket');
    assert.object(req.log, 'req.log');
    assert.object(req.pg, 'req.pg');
    assert.func(cb, 'callback');

    var b;
    var ckey = cacheKey(req.bucket.name);
    var log = req.log;
    var noCache = (req.opts && req.opts.noBucketCache);
    if (!noCache && (b = req.bucketCache.get(ckey))) {
        req.bucket = b;
        log.debug({
            bucket: b
        }, 'loadBucket: done (cached)');
        cb(null);
        return;
    }

    b = req.bucket.name;
    var pg = req.pg;
    var q;
    var row;
    var sql = util.format(('SELECT *, \'%s\' AS req_id FROM buckets_config ' +
                           'WHERE name=$1'),
                          req.req_id);

    log.debug({
        bucket: b
    }, 'loadBucket: entered');

    cb = once(cb);

    function handleQueryResults() {
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
                cb(new BucketNotFoundError(req.bucket.name));
            } else {
                var r = row;
                req.bucket = {
                    name: r.name,
                    index: JSON.parse(r.index),
                    pre: JSON.parse(r.pre).map(evalTrigger),
                    post: JSON.parse(r.post).map(evalTrigger),
                    options: JSON.parse(r.options || {}),
                    mtime: new Date(r.mtime)
                };
                if (r.reindex_active) {
                    req.bucket.reindex_active = JSON.parse(r.reindex_active);
                }

                var keys = Object.keys(req.bucket.index || {});
                req.bucket._indexKeys = keys.map(function (k) {
                    return ({
                        key: k,
                        lcKey: k.toLowerCase()
                    });
                });

                req.bucketCache.set(ckey, req.bucket);
                log.debug({
                    bucket: req.bucket
                }, 'loadBucket: done');
                cb(null);
            }
        });
    }

    if (req.explain) {
        control.runExplain(req.pg, sql, [b], function () {
            q = pg.query(sql, [b]);
            handleQueryResults();
        });
    } else {
        q = pg.query(sql, [b]);
        handleQueryResults();
    }
}


function shootdownBucket(req) {
    assert.object(req.bucketCache);
    assert.object(req.bucket);
    assert.string(req.bucket.name);

    req.log.debug('bucketCache shootdown', { bucket: req.bucket.name });
    var ckey = cacheKey(req.bucket.name);
    req.bucketCache.del(ckey);
}


function verifyBucket(req, cb) {
    if (!req.previous) {
        cb();
        return;
    }
    var rowVer = parseInt(req.previous._rver || '0', 10);
    var bucketVer = parseInt(req.bucket.options.version, 10);
    if (rowVer > bucketVer) {
        // The row we just fetched has a bucket version higher than what was
        // retrieved from the bucket cache.  Shoot down the old entry and
        // refetch so we can continue this action with a correct bucket schema.
        shootdownBucket(req);
        loadBucket(req, cb);
        return;
    }
    cb();
}


/**
 * Fetch all of the fields being reindexed for a bucket, so that we can
 * more easily error out when they're used, or ignore them when converting
 * a row fetched from Postgres into an object.
 */
function getReindexingFields(bucket) {
    assert.object(bucket, 'bucket');

    var ignore = [];

    if (bucket.reindex_active) {
        Object.keys(bucket.reindex_active).forEach(function (ver) {
            ignore = ignore.concat(bucket.reindex_active[ver]);
        });
    }

    return ignore;
}


function rowToObject(bucket, ignore, row) {
    assert.object(bucket, 'bucket');
    assert.arrayOfString(ignore, 'ignore');
    assert.object(row, 'row');

    var obj = {
        bucket: bucket.name,
        key: row._key,
        value: JSON.parse(row._value),
        _id: row._id,
        _etag: row._etag,
        _mtime: parseInt(row._mtime, 10),
        _txn_snap: row._txn_snap,
        _count: parseInt(row._count, 10)
    };

    /*
     * Moray supports 'update', which updates the Postgres columns, but not
     * the serialized JSON. Here, we do the following:
     *
     * - If the PG column is listed in 'ignore', do nothing. This indicates
     *   that the column is being reindexed, and we can't trust the data in
     *   the PG column.
     * - If the PG column is null, delete the returned value.
     * - If the serialized data is present and an Array, then:
     *   - Copy the PG column when the index type is an array
     *   - Ignore the PG column when the index type is a scalar. This is to
     *     avoid corrupting the original array that has been converted into
     *     the scalar type. The only system that relies on this behaviour is
     *     UFDS, which uses arrays with "string" indexes. This results in
     *     things like [ "a", "b" ] turning into "a,b".
     * - Otherwise, blindly overwrite.
     */
    bucket._indexKeys.forEach(function (key) {
        var k = key.key;
        var v = row[key.lcKey];
        if (ignore.indexOf(k) !== -1) {
            return;
        }
        if (v === undefined || v === null) {
            if (obj.value[k] !== null) {
                delete obj.value[k];
            }
        } else if (Array.isArray(obj.value[k])) {
            if (TYPES[bucket.index[k].type].array) {
                obj.value[k] = v;
            }
        } else {
            obj.value[k] = v;
        }
    });

    return (obj);
}


function runPostChain(req, cb) {
    if (req.bucket.post.length === 0)
        return (cb());

    var cookie = {
        bucket: req.bucket.name,
        id: req.value ? req.value._id : (req._id || -1),
        key: req.key,
        log: req.log,
        pg: req.pg,
        schema: req.bucket.index,
        value: req.value,
        headers: req.opts.headers || {},
        update: (req.previous) ? true : false
    };
    var log = req.log;

    log.debug('runPostChain: entered');

    vasync.pipeline({
        funcs: req.bucket.post,
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


function selectForUpdate(req, cb) {
    var bucket = req.bucket.name;
    var key = req.key;
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    var sql = util.format(('SELECT *, \'%s\' AS req_id FROM %s WHERE ' +
                           '_key=$1 FOR UPDATE'),
                          req.req_id, bucket);

    log.debug({
        bucket: bucket,
        key: key
    }, 'selectForUpdate: entered');

    function handleQueryResults() {
        q.once('error', function (err) {
            log.debug(err, 'selectForUpdate: failed');
            cb(err);
        });

        q.once('row', function (r) {
            row = r;
        });

        q.once('end', function (result) {
            if (row)
                req.previous = row;

            log.debug({
                previous: req.previous || null
            }, 'selectForUpdate: done');
            cb();
        });
    }

    if (req.explain) {
        control.runExplain(req.pg, sql, [key], function () {
            q = pg.query(sql, [key]);
            handleQueryResults();
        });
    } else {
        q = pg.query(sql, [key]);
        handleQueryResults();
    }
}


/**
 * Extracts the indexable fields of an input object and converts their values
 * into appropriate Postgres parameters for the index.
 *
 * - "schema", the index schema for the bucket the object is destined for
 * - "object", an object containing the fields we want to insert into the bucket
 */
function indexObject(schema, object) {
    assert.object(schema, 'schema');
    assert.object(object, 'object');

    var ndx = {};

    Object.keys(schema).forEach(function _index(k) {
        var s = schema[k];
        var value = object[k];

        if (!s || value === undefined) {
            return;
        }

        var t = s.type;
        if (!TYPES.hasOwnProperty(t)) {
            throw new InvalidIndexTypeError(k, t);
        }

        if (value === null) {
            ndx[k] = null;
        } else if (TYPES[t].array) {
            var scalarType = t.slice(1, -1);
            if (Array.isArray(value)) {
                var vals = [];
                value.forEach(function (v2) {
                    vals.push(_mapType(scalarType, v2, k, true));
                });
                ndx[k] = '{' + vals.join(', ') + '}';
            } else {
                ndx[k] = '{' + _mapType(scalarType, value, k, true) + '}';
            }
        } else {
            ndx[k] = _mapType(t, value, k);
        }
    });

    return (ndx);
}

/*
 * Returns an object that has two properties:
 * - unindexedFields: an array of string representing the names of fields that
 * don't have any index
 * - reindexingFields: an array of string representing the names of fields that
 * have an index, but whose index is being reindexed
 *
 */
function getUnusableIndexes(filter, bucket, log) {
    assert.object(filter, 'filter');
    assert.object(bucket, 'bucket');

    var bucketIndex = [];
    var unindexedFields = [];
    var reindexingFields = [];
    var bucketReindexActive = getReindexingFields(bucket);

    log.debug({bucket: bucket}, 'bucket object');

    if (bucket.index !== undefined) {
        bucketIndex = Object.keys(bucket.index);
    }

    var fieldsUsedInFilter = {};

    filter.forEach(function addUsedFields(filterItem) {
        assert.object(filterItem, 'filterItem');
        if (filterItem.filter !== undefined ||
            filterItem.filters !== undefined) {
            /*
             * We're not interested in non-leaf filters (not, and, or, etc.).
             */
            assert.equal(filterItem.attribute, undefined);
            return;
        }

        assert.string(filterItem.attribute, 'filterItem.attribute');
        fieldsUsedInFilter[filterItem.attribute] = true;
    });

    log.debug({
        fieldsUsedInFilter: fieldsUsedInFilter,
        bucketIndex: bucketIndex,
        bucketReindexActive: bucketReindexActive
    }, 'fields used in filter');

    Object.keys(fieldsUsedInFilter).forEach(checkIndexUsability);

    return {
        unindexedFields: unindexedFields,
        reindexingFields: reindexingFields
    };

    function checkIndexUsability(fieldName) {
        /*
         * If the filter field is part of the set of indexes that are usable by
         * any moray bucket from the time it's created (e.g _mtime, _key, etc.),
         * return early and consider that field has a usable Postgres index.
         */
        if (INTERNAL_FIELDS.indexOf(fieldName) !== -1) {
            return;
        }

        if (bucketIndex.indexOf(fieldName) === -1 &&
            unindexedFields.indexOf(fieldName) === -1) {
            unindexedFields.push(fieldName);
        }

        if (bucketReindexActive.indexOf(fieldName) !== -1 &&
            reindexingFields.indexOf(fieldName) === -1) {
            reindexingFields.push(fieldName);
        }
    }
}


function checkOnlyUsableIndexes(req, cb) {
    assert.object(req, 'req');
    assert.func(cb, 'cb');

    var log = req.log;
    var unusable = getUnusableIndexes(req.filter, req.bucket, log);

    if (unusable.unindexedFields.length > 0 ||
        unusable.reindexingFields.length > 0) {
        log.error({ unusable: unusable }, 'filter references unusable indexes');
        cb(new NotIndexedError({}, req.bucket.name, req.rawFilter, {
            unindexedFields: unusable.unindexedFields,
            reindexingFields: unusable.reindexingFields
        }));
        return;
    }

    log.trace('filter does not reference unusable indexes');

    cb();
}


///--- Exports

module.exports = {
    parseFilter: parseFilter,
    decorateFilter: decorateFilter,
    buildWhereClause: buildWhereClause,
    cacheKey: cacheKey,
    checkEtag: checkEtag,
    evalTrigger: evalTrigger,
    loadBucket: loadBucket,
    shootdownBucket: shootdownBucket,
    verifyBucket: verifyBucket,
    stdOutput: stdOutput,
    rowToObject: rowToObject,
    runPostChain: runPostChain,
    selectForUpdate: selectForUpdate,
    indexObject: indexObject,
    checkOnlyUsableIndexes: checkOnlyUsableIndexes,
    getReindexingFields: getReindexingFields,
    getUnusableIndexes: getUnusableIndexes
};
