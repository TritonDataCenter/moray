/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

var util = require('util');

var jsprim = require('jsprim');
var once = require('once');
var vasync = require('vasync');
var clone = require('clone');

var common = require('./common');
var control = require('../control');
var dtrace = require('../dtrace');

var mod_errors = require('../errors');
var BucketNotFoundError = mod_errors.BucketNotFoundError;
var BucketVersionError = mod_errors.BucketVersionError;
var InvalidBucketUpdateError = mod_errors.InvalidBucketUpdateError;

var typeToPg = require('../pg').typeToPg;


///--- Globals

var ARGS_SCHEMA = [
    { name: 'name', type: 'string' },
    { name: 'config', type: 'object' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    common.validateBucket,
    loadBucket,
    ensureReindexProperty,
    calculateDiff,
    ensureRowVer,
    updateConfig,
    addColumns,
    createIndexes,
    createUniqueIndexes,
    dropIndexes,
    common.shootdownBucket
];


///--- Helpers

/**
 * Consolidate existing reindex data with new fields.
 *
 * @param {Object} old Existing reindex data
 * @param {Integer} ver New bucket version number
 * @param {Object} fields Added index fields, name->type map
 * @return {Object} Consolidated reindex data
 */
function consolidateReindex(old, ver, fields) {
    var result = clone(old);
    var cur = result[ver] || [];
    result[ver] = cur.concat(fields.filter(function (field) {
        return (cur.indexOf(field) === -1);
    }));
    return result;
}

///--- Handlers

function loadBucket(req, cb) {
    var b = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    var sql = util.format('SELECT * FROM buckets_config WHERE name=\'%s\'',
                          b.name);

    cb = once(cb);

    log.debug({
        bucket: b.name
    }, 'loadBucket: entered');

    q = pg.query(sql);
    q.once('error', function (err) {
        log.debug({
            bucket: b.name,
            err: err
        }, 'loadIndexes: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function (r) {
        if (!row) {
            cb(new BucketNotFoundError(req.bucket.name));
            return;
        }

        var v = parseInt(((JSON.parse(row.options) || {}).version || 0),
                         10);
        var v2 = req.bucket.options.version || 0;
        req.index = JSON.parse(row.index);

        // Needed for reindexing details
        req.raw_bucket = row;

        // Note v=0 is a legacy hack here that you get when you make a
        // bucket with no version. If you have the version set to zero
        // in the DB, we go ahead and overwrite regardless.
        if (v !== 0 && v >= v2) {
            log.warn({
                bucket: b.name,
                oldVersion: v,
                newVersion: v2
            }, 'updateBucket: new version <= existing version');
            cb(new BucketVersionError(req.bucket.name, v, v2));
            return;
        }

        log.debug({
            bucket: b.name,
            oldIndex: req.index
        }, 'loadBucket: done');
        cb();
    });
}


function ensureReindexProperty(req, cb) {
    if (!req.raw_bucket.hasOwnProperty('reindex_active')) {
        // pre-reindexing bucket table requires the column added

        var sql = 'ALTER TABLE buckets_config ' +
            'ADD COLUMN reindex_active TEXT';
        var q = req.pg.query(sql);

        q.once('error', function (err) {
            req.log.debug({
                bucket: req.bucket.name,
                err: err
            }, 'loadIndexes: failed');
            cb(err);
        });
        q.once('end', function () {
            req.reindex_active = {};
            cb(null);
        });
        return;
    } else {
        // extract for later use
        req.reindex_active =
            JSON.parse(req.raw_bucket.reindex_active) || {};
    }
    cb(null);
}


/**
 * Determine what changes we need to make to Postgres in order to
 * match the new bucket configuration. If someone tries to update
 * the bucket in a forbidden manner (making something unique that
 * previously wasn't, for example), then return an error.
 */
function calculateDiff(req, cb) {
    var fields, i, k;
    var diff = {
        columns: {
            add: []
        },
        indexes: {
            add: [],
            del: []
        }
    };
    var next = req.bucket.index;
    var prev = req.index;

    fields = Object.keys(next);
    for (i = 0; i < fields.length; i++) {
        k = fields[i];

        if (!jsprim.hasKey(prev, k)) {
            diff.columns.add.push(k);
            diff.indexes.add.push(k);
            continue;
        }

        if (prev[k].type !== next[k].type) {
            cb(new InvalidBucketUpdateError(
                'Cannot change type of index "%s"', k));
            return;
        }

        if (!prev[k].unique && next[k].unique) {
            cb(new InvalidBucketUpdateError(
                'Cannot update index "%s" to be unique', k));
            return;
        }

        if (prev[k].unique && !next[k].unique) {
            cb(new Error('XXX: implement'));
            return;
        }

        if (prev[k].pgIndexDisabled && !next[k].pgIndexDisabled) {
            diff.indexes.add.push(k);
        }

        if (!prev[k].pgIndexDisabled && next[k].pgIndexDisabled) {
            diff.indexes.del.push(k);
        }
    }

    fields = Object.keys(prev);
    for (i = 0; i < fields.length; i++) {
        k = fields[i];

        if (!jsprim.hasKey(next, k)) {
            cb(new InvalidBucketUpdateError(
                'Cannot remove index "%s"', k));
            return;
        }
    }

    req.diff = diff;

    req.log.debug({
        bucket: req.bucket.name,
        diff: req.diff
    }, 'calculateDiff: done');
    cb();
}


function ensureRowVer(req, cb) {
    // If a reindex operation has been requested, updated/pending rows will
    // need a _rver column to track completion.  Since this will be in use as
    // soon as the bucket record is updated with the reindexing operation, it
    // _must_ exist beforehand.

    if (req.opts.no_reindex || !req.bucket.options.version) {
        // skip if bucket is versionless or reindexing excluded
        cb(null);
        return;
    }

    cb = once(cb);
    var b = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var sql, q;

    log.debug({
        bucket: b.name
    }, 'ensureRowVer: entered');

    vasync.pipeline({
        funcs: [
            function checkCol(arg, callback) {
                sql = 'SELECT column_name FROM information_schema.columns ' +
                    'WHERE table_name = $1 AND column_name = $2';
                q = pg.query(sql, [b.name, arg.colName]);
                q.on('error', callback);
                q.once('row', function () {
                    arg.colExists = true;
                });
                q.once('end', function () {
                    callback(null);
                });
            },
            function addCol(arg, callback) {
                if (arg.colExists) {
                    callback(null);
                    return;
                }
                sql = util.format('ALTER TABLE %s ADD COLUMN ', b.name) +
                    arg.colName + ' INTEGER';
                q = pg.query(sql);
                q.on('error', callback);
                q.once('end', callback.bind(null, null));
            },
            function checkIdx(arg, callback) {
                sql = 'SELECT indexname FROM pg_catalog.pg_indexes ' +
                    'WHERE tablename = $1 AND indexname = $2';
                q = pg.query(sql, [b.name, arg.idxName]);
                q.on('error', callback);
                q.once('row', function () {
                    arg.idxExists = true;
                });
                q.once('end', function () {
                    callback(null);
                });
            },
            function addIdx(arg, callback) {
                if (arg.idxExists) {
                    callback(null);
                    return;
                }
                common.createIndexes({
                    bucket: b.name,
                    log: log,
                    pg: pg,
                    indexes: [
                        {name: arg.colName, type: 'BTREE'}
                    ]
                }, callback);
            }
        ],
        arg: {
            colName: '_rver',
            idxName: b.name + '__rver_idx',
            colExists: false,
            idxExists: false
        }
    }, function (err, res) {
        log.debug({
            bucket: b.name,
            err: err
        }, 'ensureRowVer: failed');
        cb(err);
    });
}


function updateConfig(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;

    var idx = 1;
    var sql = 'UPDATE buckets_config SET index=$1';
    var values = [JSON.stringify(bucket.index)];

    sql += ', pre=$' + (++idx);
    values.push(JSON.stringify((bucket.pre || []).map(function (f) {
        return (f.toString());
    })));
    sql += ', post=$' + (++idx);
    values.push(JSON.stringify((bucket.post || []).map(function (f) {
        return (f.toString());
    })));
    sql += ', options=$' + (++idx);
    values.push(JSON.stringify(bucket.options || {}));
    if (!req.opts.no_reindex && req.bucket.options.version) {
        sql += ', reindex_active=$' + (++idx);
        values.push(consolidateReindex(req.reindex_active,
            req.bucket.options.version, req.diff.columns.add));
    }
    sql += ' WHERE name=$' + (++idx);
    values.push(bucket.name);

    cb = once(cb);

    log.debug({
        bucket: bucket.name,
        values: values
    }, 'updateConfig: entered');
    q = pg.query(sql, values);

    q.once('error', function (err) {
        log.debug({
            bucket: bucket.name,
            err: err
        }, 'updateConfig: failed');
        cb(err);
    });

    q.once('end', function () {
        log.debug({
            bucket: bucket.name
        }, 'updateConfig: done');
        cb();
    });
}


function addColumns(req, callback) {
    if (req.diff.columns.add.length === 0) {
        callback();
        return;
    }

    var log = req.log;
    var pg = req.pg;
    var sql = util.format('ALTER TABLE %s ADD COLUMN ', req.bucket.name);

    log.debug({
        bucket: req.bucket.name,
        add: req.diff.columns.add
    }, 'addColumns: entered');
    vasync.forEachParallel({
        func: function _drop(c, cb) {
            var str = sql + c +
                ' ' + typeToPg(req.bucket.index[c].type);
            log.debug({
                bucket: req.bucket.name,
                sql: str
            }, 'addColumns: adding column');
            var q = pg.query(str);
            q.once('error', cb);
            q.once('end', function (_) {
                cb();
            });
        },
        inputs: req.diff.columns.add
    }, function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'addColumns: %s', err ? 'failed' : 'done');
        callback(err);
    });
}


function createIndexes(req, cb) {
    cb = once(cb);

    if (req.diff.indexes.add.length === 0) {
        cb();
        return;
    }

    var add = req.diff.indexes.add.filter(function (k) {
        return (!req.bucket.index[k].unique);
    }).map(function (k) {
        return common.mapIndexType(req.bucket.index, k);
    });

    if (add.length === 0) {
        cb();
        return;
    }

    common.createIndexes({
        bucket: req.bucket.name,
        log: req.log,
        pg: req.pg,
        indexes: add
    }, cb);
}


function dropIndexes(req, cb) {
    var del = req.diff.indexes.del.filter(function (k) {
        /*
         * Never drop indexes for unique columns, since the
         * index is what guarantees uniqueness.
         */
        return (!req.bucket.index[k].unique);
    });

    if (del.length === 0) {
        cb();
        return;
    }

    common.dropIndexes({
        bucket: req.bucket.name,
        log: req.log,
        pg: req.pg,
        indexes: del
    }, cb);
}


function createUniqueIndexes(req, cb) {
    cb = once(cb);

    if (req.diff.indexes.add.length === 0) {
        cb();
        return;
    }

    var add = req.diff.indexes.add.filter(function (k) {
        return (req.bucket.index[k].unique);
    }).map(function (k) {
        return common.mapIndexType(req.bucket.index, k);
    });

    if (add.length === 0) {
        cb();
        return;
    }

    common.createIndexes({
        bucket: req.bucket.name,
        log: req.log,
        pg: req.pg,
        unique: true,
        indexes: add
    }, cb);
}


function update(options) {
    control.assertOptions(options);

    function _update(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var name = argv[0];
        var cfg = argv[1];
        var opts = argv[2];

        var req = control.buildReq(opts, rpc, options);
        var bucket = {
            name: name,
            index: cfg.index || {},
            pre: cfg.pre || [],
            post: cfg.post || [],
            options: cfg.options || {}
        };
        bucket.options.version = bucket.options.version || 0;
        req.bucket = bucket;

        req.log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, 'updateBucket: entered');
        dtrace['updatebucket-start'].fire(function () {
            return ([req.msgid, req.req_id, req.bucket.name]);
        });

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbProbe: function () {
                return ([req.msgid, req.req_id, req.bucket.name]);
            }
        });
    }

    return (_update);
}



///--- Exports

module.exports = {
    update: update
};
