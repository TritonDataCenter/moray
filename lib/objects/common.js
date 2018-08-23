/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var assert = require('assert-plus');
var clone = require('clone');
var filters = require('moray-filter');
var ip6addr = require('ip6addr');
var jsprim = require('jsprim');
var macaddr = require('macaddr');
var once = require('once');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');
var vm = require('vm');
var VError = require('verror');

var mod_errors = require('../errors');
var BucketNotFoundError = mod_errors.BucketNotFoundError;
var NotIndexedError = mod_errors.NotIndexedError;
var InvalidIndexTypeError = mod_errors.InvalidIndexTypeError;
var InvalidRequireError = mod_errors.InvalidRequireError;
var InvalidQueryError = mod_errors.InvalidQueryError;
var EtagConflictError = mod_errors.EtagConflictError;

var pgCommon = require('../pg');
var typeToPg = pgCommon.typeToPg;

var VE = verror.VError;

var TYPES = require('../types').TYPES;

var DATERANGE_RE = /^([([])([\d:.TZ+-]+)?,([\d:.TZ+-]+)?([)\]])$/;
var NUMRANGE_RE = /^[([](-?\d+(?:\.\d+)?)?,(-?\d+(?:\.\d+)?)?[)\]]$/;
var ISO8601_RE = /^(?:[-+]00)?\d+-\d+-\d+T\d+:\d+:\d+(?:.\d+)?Z$/;
var UUID_RE = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;


// --- Globals

var INTERNAL_FIELDS = ['_etag', '_key', '_id', '_mtime', '_txn_snap'];

/*
 * MORAY OBJECT IDENTIFIERS: THE "_id" PROPERTY
 *
 * Moray provides several internal properties on each object in a bucket; e.g.,
 * "_mtime", "_etag", etc.  One of these properties is "_id", an integer
 * assigned from a monotonically increasing sequence to objects at the time of
 * initial put.  The "_id" value for an object in a bucket is unique amongst
 * all objects in that bucket.  If an object is updated, the "_id" value will
 * not change.  If an object is deleted, that "_id" value is not reused; a new
 * "_id" value will be assigned for subsequent puts that reuse the same key.
 *
 * Though the sequence from which these identifiers are assigned is capable of
 * generating up to 64-bit signed numbers (a maximum of ~9.2e18), due
 * to an accident of history the "_id" column in which the value is stored
 * is often of the INTEGER type.  These numbers are signed 32-bit quantities,
 * and thus allow a little more than two billion rows to be inserted over the
 * lifetime of the bucket.
 *
 * New buckets are created with the BIGINT type for the "_id" column, and are
 * not subject to this limitation.
 *
 *
 * CONVERTING "_id" TO BIGINT (WITH DOWNTIME)
 *
 * For any deployment which can take an outage for maintenance tasks, operators
 * are encouraged to alter the type of the "_id" column to expand it to BIGINT.
 * Depending on the size of a particular Moray bucket, this may take some time.
 * This operation would be of the form:
 *
 *      ALTER TABLE <bucket> ALTER COLUMN _id TYPE BIGINT;
 *
 *
 * ENABLING THE EXTENDED IDENTIFIER MECHANISM (WITHOUT DOWNTIME)
 *
 * For a deployment with a large bucket where downtime for maintenance is not
 * permissible, a second option is available.  A new column can be created with
 * the name "_idx" and a default value of NULL, and indexed for the same kind
 * of searches as are possible today on "_id".  Creating the column with a
 * default value of NULL is effectively a metadata-only operation and is not
 * expensive.  Use of CREATE INDEX CONCURRENTLY may take a long time, but will
 * not prevent normal data operations on the table.
 *
 * Perform both of these operations as the first step in enabling extended ID
 * support:
 *
 *      ALTER TABLE <bucket> ADD COLUMN _idx BIGINT;
 *
 *      CREATE INDEX CONCURRENTLY <bucket>__idx_idx ON <bucket>
 *          USING BTREE (_idx) WHERE _idx IS NOT NULL;
 *
 * Once this new column is created and indexed, Moray will (upon the next
 * refresh of the cached bucket configuration) detect and include "_idx" in any
 * query that filters or sorts on the newly virtualised "_id" property; see
 * "compileQuery()" below.
 *
 * After all Moray processes in a shard are aware of the new column and index,
 * the second and final step in enabling the extended identifier facility is to
 * move the default value configuration from the "_id" column to the "_idx"
 * column.  This operation is also effectively meta-data only, and should not
 * have an impact on the running system:
 *
 *      ALTER TABLE <bucket>
 *          ALTER COLUMN _id DROP DEFAULT,
 *          ALTER COLUMN _idx SET DEFAULT nextval('<bucket>_serial'::regclass);
 *
 * This last step is irreversible and will cause PostgreSQL to store all future
 * identifiers for new objects in the "_idx" column.  Moray virtualises access
 * to the "_id" property: usage in a predicate or sort option will result in a
 * composite WHERE or ORDER BY clause which produces the expected results as if
 * there was still only one column.
 *
 * NOTE: any deviation from the above procedure may produce undefined results
 * in subsequent queries that involve the "_id" property.
 */
var EXTENDED_ID = '_idx';

// --- Internal Helpers

/*
 * Validate an ISO 8601 timestamp, and return a normalized representation of it.
 *
 * Note that while our regular expression supports BCE dates, Postgres will not
 * accept them due to broken handling of BCE timestamps. Instead, the error
 * message "time zone displacement out of range" will be returned to the
 * consumer. Postgres will however accept its own unique method of writing
 * timestamps with a "BC" suffix.
 *
 * While the node-postgres client will parse these when it encounters them, and
 * it will convert Date objects into Postgres's mangled interpretation of ISO
 * 8601, it doesn't generate the "BC" suffix. (It also doesn't convert Date
 * objects in Arrays.) Maybe someday it will gain support for it, at which
 * point we maybe can use Date objects in our query parameters array.
 *
 * It is, of course, possible that this is entirely moot: Triton and Manta will
 * most likely never actually care about dates before the vulgar era.
 */
function parseISO8601(value) {
    if (!ISO8601_RE.test(value)) {
        throw new VError('invalid ISO 8601 timestamp: %j', value);
    }

    return (new Date(value)).toISOString();
}

/*
 * We don't do any error checking here, since some Moray consumers have
 * ended up accidentally relying on the value getting turned into NaN
 * here, and then NULL when sent to Postgres.
 */
function parseNumber(value) {
    var v = value;

    if (typeof (v) !== 'number') {
        v = parseFloat(value, 10);
    }

    return v;
}

function parseDateRange(value) {
    var lower = '';
    var upper = '';

    var m = DATERANGE_RE.exec(value);
    if (m === null) {
        throw new VError('invalid date range: %j', value);
    }

    if (m[2]) {
        lower = parseISO8601(m[2]);
    }

    if (m[3]) {
        upper = parseISO8601(m[3]);
    }

    return m[1] + lower + ',' + upper + m[4];
}

function parseNumericRange(value) {
    if (NUMRANGE_RE.test(value)) {
        return value;
    } else {
        throw new VError('invalid numeric range: %j', value);
    }
}

function parseUUID(value) {
    if (UUID_RE.test(value)) {
        return value;
    } else {
        throw new VError('invalid uuid: %j', value);
    }
}

function parseBoolean(value) {
    var ret = value.toString().toUpperCase();
    if (ret !== 'TRUE' && ret !== 'FALSE') {
        throw new VError('invalid boolean: %j', value);
    }

    return ret;
}

function _parse(val, filter, type) {
    var array = TYPES[type].array;
    var value;

    switch (type) {
    case '[boolean]':
    case 'boolean':
        value = parseBoolean(val);
        filter.value = (value === 'TRUE');
        break;

    case '[date]':
    case 'date':
        filter.value = parseISO8601(val);
        value = new Date(filter.value);
        break;

    case '[daterange]':
    case 'daterange':
        filter.value = value = parseDateRange(val);
        break;

    case '[number]':
    case 'number':
        filter.value = value = parseNumber(val);
        break;

    case '[numrange]':
    case 'numrange':
        filter.value = value = parseNumericRange(val);
        break;

    case '[ip]':
    case 'ip':
        filter.value = ip6addr.parse(val);
        value = filter.value.toString();
        break;

    case '[mac]':
    case 'mac':
        filter.value = macaddr.parse(val);
        value = filter.value.toString();
        break;

    case '[subnet]':
    case 'subnet':
        filter.value = ip6addr.createCIDR(val);
        value = filter.value.toString();
        break;

    case '[uuid]':
    case 'uuid':
        value = parseUUID(val);
        break;

    case '[string]':
    case 'string':
        value = val;
        break;

    default:
        value = val;
        break;
    }

    return {
        isArray: array,
        value: value
    };
}

/**
 * Parse the right-hand side of a filter, and return an object with the fields
 * "isArray" (whether this is an array type) and "value", the value to pass in
 * the array of query parameters.
 *
 * We also update the filter.value field passed in to a normalized or object
 * representation of the value, to use later when post-processing the objects
 * return from Postgres.
 */
function parse(val, filter, type) {
    try {
        return _parse(val, filter, type);
    } catch (e) {
        throw new InvalidQueryError(e, filter.toString());
    }
}

/**
 * This function is used during processing of the parsed Moray filter to:
 *
 *   - Convert the string value into what will be sent to Postgres as a
 *     parameter to the generated SQL query.
 *   - Update the filter object with the JavaScript value to compare to.
 *   - Determine if this is an array type.
 */
function _value(schema, key, val, filter) {
    if (!jsprim.hasKey(schema, key)) {
        switch (key) {
        case '_id':
        case '_txn_snap':
        case '_mtime':
            return parse(val, filter, 'number');
        case '_etag':
        case '_key':
            return parse(val, filter, 'string');
        default:
            return {
                isArray: false,
                value: undefined
            };
        }
    }

    return parse(val, filter, schema[key].type);
}


/**
 * This function takes care of converting a filter into a SQL query that we can
 * pass to Postgres. When it encounters an attribute that can't be searched in
 * the database, we throw a NotIndexedError. If the operation is invalid for the
 * type or the filter's value is malformed/nonsensical, then we throw an
 * InvalidQueryError.
 *
 * One tricky thing to keep in mind while reading this code: we always generate
 * comparisons with the column name on the right-hand side, which is the
 * *opposite* of how they are written in the filter. This is because Postgres
 * requires the ANY keyword to always appear on the right-hand side of an
 * operation. Thus, the following filter:
 *
 *     (&(a>=5)(b<=40))
 *
 * Turns into the second of the two equivalent expressions:
 *
 *     a >= 5 AND b <= 40
 *     5 <= a AND 40 >= b
 */
function compileQuery(b, f, args) {
    assert.object(b, 'bucket');
    assert.string(b.name, 'bucket.name');
    assert.object(b.index, 'bucket.index');
    assert.bool(b.hasExtendedId, 'b.hasExtendedId');
    assert.object(f, 'query');
    assert.array(args, 'args');

    var clause = '';
    var i;
    var v;

    function _mapOp(direction) {
        var ltype, rtype, op;

        if (!jsprim.hasKey(b.index, f.attribute)) {
            throw new NotIndexedError(b.name, f.toString());
        }

        ltype = b.index[f.attribute].type;
        if (!jsprim.hasKey(TYPES, ltype)) {
            throw new InvalidQueryError(f.toString());
        }

        if (TYPES[ltype][direction] === null) {
            throw new InvalidQueryError(f.toString());
        }

        rtype = TYPES[ltype][direction].type;
        op = TYPES[ltype][direction].op;

        v = parse(f.value, f, rtype);

        args.push(v.value);

        if (TYPES[ltype].array) {
            clause += util.format('$%d::%s %s ANY(%s)',
                args.length, typeToPg(rtype), op, f.attribute);
        } else {
            clause += util.format('$%d::%s %s %s',
                args.length, typeToPg(rtype), op, f.attribute);
        }
    }

    function _append(op) {
        /*
         * If this bucket has the extended ID column ("_idx"), and this is a
         * filter on the virtual "_id" property, we need to expand the
         * predicate to cover both the "_id" and "_idx" columns in the table.
         */
        var extid = (f.attribute === '_id' && b.hasExtendedId);

        v = _value(b.index, f.attribute, f.value, f);
        if (v.value !== undefined) {
            args.push(v.value);

            if (extid) {
                clause += ' (';
            }

            clause += ' ( ';
            if (v.isArray) {
                if (op === '=') {
                    clause += f.attribute + ' @> ARRAY[$' + args.length + ']::'
                        + typeToPg(b.index[f.attribute].type);
                } else {
                    clause += '$' + args.length + ' ' + op +
                        ' ANY(' + f.attribute + ')';
                }
            } else {
                clause += '$' + args.length + ' ' + op + ' ' + f.attribute;
                if (extid) {
                    /*
                     * PostgreSQL appears to select a type for a parameter
                     * based on the column used in the binary expression.  The
                     * "_id" column is probably 32 bits wide, but in an
                     * extended ID world we may wish to query it with "_id"
                     * predicates too wide for this type.  Explicitly cast to
                     * BIGINT to construct queries that will be valid for both
                     * "_id" (whether 32- or 64-bit in this deployment) and
                     * "_idx".
                     */
                    clause += '::BIGINT';
                }
                clause += ' AND ' + f.attribute + ' IS NOT NULL';
            }
            clause += ' ) ';

            if (extid) {
                clause += 'OR ( ';
                clause += '$' + args.length + ' ' + op + ' ' + EXTENDED_ID;
                clause += ' AND ' + EXTENDED_ID + ' IS NOT NULL';
                clause += ' ) ) ';
            }
        }
    }

    function _substr(op) {
        var _like_tmp = '';
        var _v;
        var valid = true;

        if (f.initial) {
            _v = _value(b.index, f.attribute, f.initial, f);
            if (_v.isArray) {
                throw new NotIndexedError(b.name, f.toString());
            }
            if (_v.value === undefined) {
                valid = false;
            }
            _like_tmp += _v.value + '%';
        }

        f.any.forEach(function (any) {
            _v = _value(b.index, f.attribute, any, f);
            if (_v.isArray) {
                throw new NotIndexedError(b.name, f.toString());
            }
            if (_v.value === undefined) {
                valid = false;
            }
            _like_tmp += '%' + _v.value + '%';
        });

        if (f.final) {
            _v = _value(b.index, f.attribute, f.final, f);
            if (_v.isArray) {
                throw new NotIndexedError(b.name, f.toString());
            }
            if (_v.value === undefined) {
                valid = false;
            }
            _like_tmp += '%' + _v.value + '%';
        }

        if (valid) {
            args.push(_like_tmp);
            clause += ' ( ';
            clause += f.attribute + ' ' + op + ' $' + (args.length);
            clause += ' AND ' + f.attribute + ' IS NOT NULL';
            clause += ' ) ';
        }
    }

    switch (f.type) {
    case 'and':
        var ands = [];
        f.filters.forEach(function (_f) {
            v = compileQuery(b, _f, args);
            if (v !== '') {
                ands.push(v);
            }
        });
        if (ands.length === 0) {
            throw new NotIndexedError(b.name, f.toString());
        }

        for (i = 0; i < ands.length; i++) {
            clause += ' (' + ands[i] + ') ';
            if (i < ands.length - 1) {
                clause += 'AND';
            }
        }
        break;

    case 'or':
        var ors = [];
        f.filters.forEach(function (_f) {
            v = compileQuery(b, _f, args);
            if (v === '') {
                throw new NotIndexedError(b.name, f.toString());
            }

            ors.push(v);
        });
        if (ors.length === 0) {
            throw new NotIndexedError(b.name, f.toString());
        }

        for (i = 0; i < ors.length; i++) {
            clause += ' (' + ors[i] + ') ';
            if (i < ors.length - 1) {
                clause += 'OR';
            }
        }
        break;

    case 'not':
        v = compileQuery(b, f.filter, args);
        if (v !== '') {
            clause += ' NOT (' + v + ')';
        }
        break;

    case 'substring':
        if (!b.index[f.attribute] && !/^_\w+/.test(f.attribute)) {
            break;
        }

        _substr('LIKE');
        break;

    case 'present':
        if (b.index[f.attribute]) {
            clause += f.attribute + ' IS NOT NULL';
        }
        break;

    case 'ge':
        _append('<=');
        break;

    case 'le':
        _append('>=');
        break;

    case 'ext':
        switch (f.rule) {
        case 'overlaps':
        case 'contains':
        case 'within':
            _mapOp(f.rule);
            break;

        case 'caseIgnoreMatch':
            _append('ILIKE');
            break;

        case 'caseIgnoreSubstringsMatch':
            _substr('ILIKE');
            break;

        default:
            throw new NotIndexedError(b.name, f.toString());
        }
        break;

    case 'equal':
    default:
        _append('=');
        break;
    }

    return clause;
}


/**
 * Maps a value from the JSON object sent by the client into a value to insert
 * into a Postgres column.
 */
function _mapType(type, value, key, isArray) {
    var ret;

    switch (type) {
    case 'boolean':
        try {
            ret = parseBoolean(value);
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'date':
        try {
            ret = parseISO8601(value);
            if (!isArray) {
                ret = new Date(ret);
            }
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'daterange':
        try {
            ret = parseDateRange(value);
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'mac':
        try {
            ret = (macaddr.parse(value)).toString();
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'number':
        try {
            ret = parseNumber(value);
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'numrange':
        try {
            ret = parseNumericRange(value);
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
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
        try {
            ret = ip6addr.createCIDR(value).toString();
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
        break;

    case 'uuid':
        try {
            ret = parseUUID(value);
        } catch (e) {
            throw new InvalidIndexTypeError(e, key, type);
        }
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
 * Decorate ge/le filters to support mac type.
 */
function _matchTypeMAC(filter) {
    function matchesMAC(target) {
        var self = this;
        var tv = filters.getAttrValue(target, this.attribute);

        return filters.testValues(function (v) {
            try {
                var mac = macaddr.parse(v);
                switch (self.type) {
                case 'ge':
                    return (mac.compare(self.value) >= 0);
                case 'le':
                    return (mac.compare(self.value) <= 0);
                case 'equal':
                    return (mac.compare(self.value) === 0);
                default:
                    return false;
                }
            } catch (_) {
                return false;
            }
        }, tv);
    }
    if (filter.type === 'ge' ||
        filter.type === 'le' ||
        filter.type === 'equal') {
        filter.matches = matchesMAC.bind(filter);
    }
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
            } catch (_) {
                return false;
            }
        }, tv);
    }
    if (filter.type === 'ge' ||
        filter.type === 'le' ||
        filter.type === 'equal') {
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
            } catch (_) {
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

function _matchAlwaysTrue() {
    return true;
}

function _matchPgOnly(b, filter) {
    if (!jsprim.hasKey(b.index, filter.attribute)) {
        throw new NotIndexedError(b.name, filter.toString());
    }

    filter.matches = _matchAlwaysTrue;
}


// --- API

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
                case 'overlaps':
                case 'contains':
                case 'within':
                    _matchPgOnly(req.idxBucket, f);
                    break;
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

            /*
             * The "_id" property is virtual: it may be the combination of the
             * "_id" and "_idx" columns in the underlying table.  To avoid
             * confusion, we do not allow the consumer to directly use the
             * "_idx" column.
             */
            if (f.attribute === EXTENDED_ID) {
                throw new Error('filtering on "_idx" is not allowed');
            }

            /* Support correct eq/ge/le comparison for special types */
            if (req.idxBucket.index[f.attribute] !== undefined) {
                switch (req.idxBucket.index[f.attribute].type) {
                case 'mac':
                case '[mac]':
                    _matchTypeMAC(f);
                    break;
                case 'ip':
                case '[ip]':
                    _matchTypeIP(f);
                    break;
                case 'subnet':
                case '[subnet]':
                    _matchTypeSubnet(f);
                    break;

                case '[date]':
                case 'date':
                case '[daterange]':
                case 'daterange':
                case '[numrange]':
                case 'numrange':
                    _matchPgOnly(req.idxBucket, f);
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
    var args = [];
    var sql;

    // Query only against fields with valid indices
    var b = opts.idxBucket;

    try {
        sql = compileQuery(b, f, args);
        if (sql === '') {
            throw new InvalidQueryError(f.toString());
        }
        where += sql;

        if (o.sort) {
            var sorts = Array.isArray(o.sort) ? o.sort : [ o.sort ];

            if (sorts.length > 0) {
                var sort = '';
                var append = function (item) { // eslint-disable-line
                    if (item.attribute) {
                        if (sort.length > 0) {
                            sort += ', ';
                        }
                        sort += item.attribute;
                        if (item.order) {
                            sort += ' ' + item.order;
                        }
                    }
                };
                sorts.forEach(function (item) {
                    if (item.attribute === EXTENDED_ID) {
                        /*
                         * Do not allow the caller to sort by "_idx".
                         */
                        throw new Error('sorting on "_idx" is not allowed');
                    }

                    append(item);

                    assert.bool(b.hasExtendedId, 'b.hasExtendedId');
                    if (item.attribute === '_id' && b.hasExtendedId) {
                        /*
                         * This table has the extended ID column and the caller
                         * has requested a sort on the virtual "_id" property.
                         *
                         * Getting the correct sort order from the composition
                         * of "_id" and "_idx" is somewhat subtle.  By default,
                         * PostgreSQL considers NULL values as if they were
                         * larger than non-NULL values.  That is, for an ASC
                         * sort they will appear last, and for a DESC sort they
                         * will appear first.
                         *
                         * In order to get correct sort order across these two
                         * columns, the procedure for adding the extended ID
                         * column to an existing Moray bucket specifically
                         * requires both that the transition be one-way, and
                         * that all "_id" values must be smaller than any
                         * "_idx" values.  When this is true, we can sort first
                         * on "_id" (which treats all NULL values as larger
                         * than any possible number): all "_idx" rows will sort
                         * as larger.  Ties will be broken by the value of the
                         * "_idx" column, giving the overall correct sort
                         * order.
                         */
                        append({ attribute: '_idx', order: item.order });
                    }
                });
                if (sort.length > 0) {
                    where += ' ORDER BY ' + sort;
                }
            }
        }

        if (o.limit) {
            where += ' LIMIT ' + o.limit;
        } else if (!o.noLimit) {
            where += ' LIMIT ' + 1000;
        }

        if (o.offset) {
            where += ' OFFSET ' + o.offset;
        }

        opts.where = {
            clause: where,
            args: args
        };
    } catch (e) {
        log.debug(e, 'buildWhereClause: failed');
        cb(e);
        return;
    }

    cb();
}


function cacheKey(b, k) {
    assert.string(b, 'bucket');

    var str = '/' + b;
    if (k) {
        str += '/' + k;
    }

    return (str);
}


function checkEtag(req, cb) {
    var b = req.bucket;
    var etag = req.opts.etag !== undefined ? req.opts.etag : req._etag;
    var log = req.log;
    var k = req.key;
    var old = (req.previous || {})._etag;

    /*
     * The logic for etag checking below is as follows (assume 'etag' is
     * caller-specified value):
     *
     * - if the etag is 'undefined', no-op, caller doesn't care (common)
     * - if the etag is 'null', there must be no previous record
     * - otherwise, the etag has to match the existing record (if there was
     *   no existing record, that's an error)
     */
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


function checkExtendedId(req, callback) {
    var log = req.log;
    var pg = req.pg;

    log.debug({
        bucket: req.bucket.name
    }, 'checkExtendedId: entered');

    var columnExists = false;
    var indexReady = false;

    vasync.waterfall([ function (next) {
        /*
         * Check to see if the table for this bucket has the "_idx" column.  If
         * this column is present, the "_id" property of objects in this bucket
         * is virtual: the actual ID value might be in either the "_id" column
         * or the "_idx" column.
         */
        var rows = [];
        var q = pg.query([
            'SELECT',
            '    TRUE as exists',
            'FROM',
            '    pg_catalog.pg_attribute pga',
            'WHERE',
            '    pga.attrelid = \'' + req.bucket.name + '\'::regclass AND',
            '    pga.attname = \'_idx\' AND',
            '    NOT pga.attisdropped'
        ].join(' '));

        q.once('error', function (err) {
            err = new VE(err, 'check bucket "%s" for _idx column',
              req.bucket.name);
            log.error({
                bucket: req.bucket.name,
                err: err
            }, 'checkExtendedId: check for column failed');
            setImmediate(next, err);
        });

        q.on('row', function (row) {
            rows.push(row);
        });

        q.on('end', function () {
            log.debug({
                bucket: req.bucket.name,
                rows: rows
            }, 'checkExtendedId: check for column results');

            assert.arrayOfObject(rows, 'rows');
            if (rows.length !== 1) {
                setImmediate(next);
                return;
            }

            assert.strictEqual(rows[0].exists, true, 'rows[0].exists');
            columnExists = true;

            setImmediate(next);
        });
    }, function (next) {
        if (!columnExists) {
            setImmediate(next);
            return;
        }

        /*
         * Check to see if the index for the "_idx" column exists and is
         * able to be used by queries.
         */
        var rows = [];
        var q = pg.query([
            'SELECT',
            '    pgc.relname AS table_name,',
            '    pgc.oid AS table_oid,',
            '    pgci.relname AS index_name,',
            '    pgi.indexrelid AS index_oid,',
            '    pgi.indisvalid AS index_valid',
            'FROM',
            '    pg_catalog.pg_class pgc INNER JOIN',
            '    pg_catalog.pg_index pgi ON pgc.oid = pgi.indrelid INNER JOIN',
            '    pg_catalog.pg_class pgci ON pgi.indexrelid = pgci.oid',
            'WHERE',
            '    pgc.relname = \'' + req.bucket.name + '\' AND',
            '    pgci.relname = \'' + req.bucket.name + '__idx_idx\''
        ].join(' '));

        q.on('error', function (err) {
            err = new VE(err, 'check bucket "%s" for _idx index',
              req.bucket.name);
            log.error({
                bucket: req.bucket.name,
                err: err
            }, 'checkExtendedId: check for index failed');
            setImmediate(next, err);
        });

        q.on('row', function (row) {
            rows.push(row);
        });

        q.on('end', function () {
            log.debug({
                bucket: req.bucket.name,
                rows: rows
            }, 'checkExtendedId: check for index results');

            assert.arrayOfObject(rows, 'rows');
            if (rows.length !== 1) {
                setImmediate(next);
                return;
            }

            assert.bool(rows[0].index_valid, 'rows[0].index_valid');
            indexReady = rows[0].index_valid;

            setImmediate(next);
        });
    } ], function (err) {
        log.debug({
            bucket: req.bucket.name,
            columnExists: columnExists,
            indexReady: indexReady,
            err: err
        }, 'checkExtendedId: done');

        if (err) {
            setImmediate(callback, err);
            return;
        }

        /*
         * If the "_idx" column exists, and has been completely indexed, we
         * will use it in queries.
         */
        setImmediate(callback, null, (columnExists && indexReady));
    });
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
            cb(new BucketNotFoundError(req.bucket.name));
            return;
        }

        checkExtendedId(req, function (err, hasExtendedId) {
            if (err) {
                cb(err);
                return;
            }

            assert.bool(hasExtendedId, 'hasExtendedId');

            var r = row;
            req.bucket = {
                name: r.name,
                index: JSON.parse(r.index),
                pre: JSON.parse(r.pre).map(evalTrigger),
                post: JSON.parse(r.post).map(evalTrigger),
                options: JSON.parse(r.options || {}),
                mtime: new Date(r.mtime),
                hasExtendedId: hasExtendedId
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
        });
    });
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
        /*
         * The row we just fetched has a bucket version higher than what was
         * retrieved from the bucket cache.  Shoot down the old entry and
         * refetch so we can continue this action with a correct bucket schema.
         */
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


function rowExtractId(bucket, key, row) {
    assert.object(bucket, 'bucket');
    assert.bool(bucket.hasExtendedId, 'bucket.hasExtendedId');
    assert.string(key, 'key');
    assert.object(row, 'row');

    var idval;
    if (bucket.hasExtendedId) {
        /*
         * This bucket has the extended ID column ("_idx").  The value to use
         * for the "_id" property might come either from the "_id" column or
         * the "_idx" column.
         */
        if (row._id === null) {
            assert.ok(row._idx !== null, 'must have _idx if no _id');
            idval = row._idx;
        } else {
            assert.ok(row._idx === null, 'must not have _idx if _id exists');
            idval = row._id;
        }
    } else {
        /*
         * If the bucket does not have the extended ID column, use the "_id"
         * column unconditionally.
         */
        idval = row._id;
    }

    /*
     * Depending on the type of the "_id" and "_idx" column the PostgreSQL
     * client may return the value as either a number or a string.  Moray
     * clients expect that the property value will always be a number; if
     * needed, convert it now.
     */
    switch (typeof (idval)) {
    case 'number':
        return (idval);

    case 'string':
        /*
         * Parse the ID value as a positive integer.  If the integer is too
         * large to be expressed precisely in the native number type, return an
         * error.
         */
        var idnum = jsprim.parseInteger(idval, { allowSign: false,
          allowImprecise: false });
        if (typeof (idnum) === 'number') {
            return (idnum);
        }
        throw (new VE(idnum, 'invalid "_id" value (bucket "%s"; key "%s")',
          bucket.name, key));

    default:
        throw (new VE('invalid "_id" type (bucket "%s"; key "%s"): %j',
          bucket.name, key, idval));
    }
}


function rowToObject(bucket, ignore, row) {
    assert.object(bucket, 'bucket');
    assert.arrayOfString(ignore, 'ignore');
    assert.object(row, 'row');

    var obj = {
        bucket: bucket.name,
        key: row._key,
        value: JSON.parse(row._value),
        _id: rowExtractId(bucket, row._key, row),
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
    if (req.bucket.post.length === 0) {
        cb();
        return;
    }

    var cookie = {
        bucket: req.bucket.name,
        id: req.value ? req.value._id : (req._id || -1),
        key: req.key,
        log: req.log,
        pg: req.pg,
        schema: req.bucket.index,
        value: req.value,
        headers: req.opts.headers || {},
        update: !!req.previous
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
    q = pg.query(sql, [key]);

    q.once('error', function (err) {
        log.debug(err, 'selectForUpdate: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function (result) {
        if (row) {
            req.previous = row;
        }

        log.debug({
            previous: req.previous || null
        }, 'selectForUpdate: done');
        cb();
    });
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


// --- Exports

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
    rowExtractId: rowExtractId,
    runPostChain: runPostChain,
    selectForUpdate: selectForUpdate,
    indexObject: indexObject,
    checkOnlyUsableIndexes: checkOnlyUsableIndexes,
    getReindexingFields: getReindexingFields,
    getUnusableIndexes: getUnusableIndexes
};
