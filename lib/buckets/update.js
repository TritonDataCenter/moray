// Copyright 2014 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var libuuid = require('libuuid');
var once = require('once');
var vasync = require('vasync');
var clone = require('clone');

var common = require('./common');
var objCommon = require('../objects/common');
require('../errors');


///--- Globals

var sprintf = util.format;


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
    var sql = sprintf('SELECT * FROM buckets_config WHERE name=\'%s\'',
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
            log.error({
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


function calculateDiff(req, cb) {
    var diff = {
        add: [],
        del: [],
        mod: []
    };
    var next = req.bucket.index;
    var prev = req.index;

    Object.keys(next).forEach(function (k) {
        if (prev[k] === undefined) {
            diff.add.push(k);
        } else if (!deepEqual(next[k], prev[k])) {
            diff.mod.push(k);
        }
    });

    Object.keys(prev).forEach(function (k) {
        if (!next[k]) {
            diff.del.push(k);
        }
    });

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
        // TODO: include modified columns in this list?
        values.push(consolidateReindex(req.reindex_active,
                    req.bucket.options.version, req.diff.add));
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


function dropColumns(req, cb) {
    if (req.diff.del.length === 0) {
        cb();
        return;
    }

    cb = once(cb);

    var log = req.log;
    var pg = req.pg;
    var sql = sprintf('ALTER TABLE %s DROP COLUMN ', req.bucket.name);

    log.debug({
        bucket: req.bucket.name,
        del: req.diff.del.join(', ')
    }, 'dropColumns: entered');
    vasync.forEachParallel({
        func: function _drop(c, _cb) {
            _cb = once(_cb);
            var q = pg.query(sql + c);
            q.once('error', _cb);
            q.once('end', function () {
                _cb();
            });
        },
        inputs: req.diff.del
    }, function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'dropColumns: %s', err ? 'failed' : 'done');
        cb(err);
    });
}


function addColumns(req, cb) {
    if (req.diff.add.length === 0) {
        cb();
        return;
    }

    cb = once(cb);

    var log = req.log;
    var pg = req.pg;
    var sql = sprintf('ALTER TABLE %s ADD COLUMN ', req.bucket.name);

    log.debug({
        bucket: req.bucket.name,
        add: req.diff.add.join(', ')
    }, 'addColumns: entered');
    vasync.forEachParallel({
        func: function _drop(c, _cb) {
            _cb = once(_cb);
            var str = sql + c +
                ' ' + common.typeToPg(req.bucket.index[c].type);
            log.debug({
                bucket: req.bucket.name,
                sql: str
            }, 'addColumns: adding column');
            var q = pg.query(str);
            q.once('error', _cb);
            q.once('end', function () {
                _cb();
            });
        },
        inputs: req.diff.add
    }, function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'dropColumns: %s', err ? 'failed' : 'done');
        cb(err);
    });
}


function createIndexes(req, cb) {
    cb = once(cb);

    if (req.diff.add.length === 0) {
        cb();
        return;
    }

    var add = req.diff.add.filter(function (k) {
        return (!req.bucket.index[k].unique);
    }).map(common.mapIndexType.bind(null, req.bucket.index));

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


function createUniqueIndexes(req, cb) {
    cb = once(cb);

    if (req.diff.add.length === 0) {
        cb();
        return;
    }

    var add = req.diff.add.filter(function (k) {
        return (req.bucket.index[k].unique);
    }).map(common.mapIndexType.bind(null, req.bucket.index));

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
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _update(name, cfg, opts, res) {
        var log = options.log.child({
            req_id: opts.req_id || libuuid.create()
        });

        log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, 'updateBucket: entered');

        var bucket = {
            name: name,
            index: cfg.index || {},
            pre: cfg.pre || [],
            post: cfg.post || [],
            options: cfg.options || {}
        };

        bucket.options.version = bucket.options.version || 0;

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr,
                          'updateBucket: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'updateBucket: transaction started');

            vasync.pipeline({
                funcs: [
                    common.validateBucket,
                    loadBucket,
                    ensureReindexProperty,
                    calculateDiff,
                    ensureRowVer,
                    updateConfig,
                    dropColumns,
                    addColumns,
                    createIndexes,
                    createUniqueIndexes
                ],
                arg: {
                    bucket: bucket,
                    log: log,
                    pg: pg,
                    manatee: manatee,
                    opts: opts
                }
            }, common.pipelineCallback({
                log: log,
                name: 'updateBucket',
                pg: pg,
                res: res
            }));
        });
    }

    return (_update);
}



///--- Exports

module.exports = {
    update: update
};
