/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

var util = require('util');

var deepEqual = require('deep-equal');
var once = require('once');
var vasync = require('vasync');
var clone = require('clone');

var common = require('./common');
var control = require('../control');

var mod_errors = require('../errors');
var BucketNotFoundError = mod_errors.BucketNotFoundError;
var BucketVersionError = mod_errors.BucketVersionError;

var typeToPg = require('../pg').typeToPg;


///--- Handlers

function ensureReindexProperty(req, cb) {
    if (!req.raw_bucket.hasOwnProperty('reindex_active')) {
        // pre-reindexing bucket table requires the column added

        var sql = 'ALTER TABLE buckets_config ' +
                  'ADD COLUMN IF NOT EXISTS reindex_active TEXT';
        var q = req.pg.query(sql);

        q.once('error', function (err) {
            req.log.debug({
                bucket: req.bucket.name,
                err: err
            }, 'ensureReindexProperty: failed');
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


function loadBucket(req, cb) {
    if (req.prev) {
        return (cb());
    }

    cb = once(cb);

    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    const sql = 'WITH insertConfig AS (' +
                '    INSERT INTO buckets_config' +
                '    (name, index, pre, post, options)' +
                '    VALUES ($1, $2, $3, $4, $5)' +
                '    ON CONFLICT (name) DO NOTHING' +
                ') ' +
                'SELECT name, index, pre, post, options, reindex_active ' +
                'FROM buckets_config WHERE name = $1';
    var values = [
        bucket.name,
        JSON.stringify(bucket.index)
    ];
    values.push(JSON.stringify((bucket.pre || []).map(function (f) {
        return (f.toString());
    })));
    values.push(JSON.stringify((bucket.post || []).map(function (f) {
        return (f.toString());
    })));
    values.push(JSON.stringify(bucket.options || {}));

    log.debug({
        bucket: util.inspect(bucket)
    }, 'insertConfig: entered');

    q = pg.query(sql, values);

    q.once('error', function (err) {
        log.debug(err, 'insertConfig: failed');
        if (err.code === '23505') {
            cb(new BucketConflictError(err, bucket.name));
        } else {
            cb(new InternalError(err, 'unable to create bucket'));
        }
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        log.debug('loadBucket: done');
        cb();

        var v = parseInt(((JSON.parse(row.options) || {}).version || 0), 10);
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

        // Used to decide what actions to take in subsequent handlers
        req.version_changed = v !== v2
    });

    return (undefined);
}


function createSequence(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format('CREATE SEQUENCE IF NOT EXISTS %s_serial', bucket.name);

    cb = once(cb);

    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'createSequence: entered');
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'createSequence: failed');
        cb(err);
    });

    q.once('end', function () {
        log.debug('createSequence: done');
        cb();
    });
}


function createLockingSerial(req, cb) {
    if (req.bucket.options.guaranteeOrder !== true) {
        cb();
        return;
    }

    cb = once(cb);

    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format(('CREATE TABLE IF NOT EXISTS %s_locking_serial ' +
                           '(id INTEGER PRIMARY KEY)'),
                          bucket.name);

    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'createLockingSerial: entered');
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'createLockingSerial: failed');
        cb(err);
    });

    q.once('end', function () {
        sql = util.format('INSERT INTO %s_locking_serial (id) VALUES (1)',
                          bucket.name);
        q = pg.query(sql);
        q.once('error', function (err) {
            log.debug(err, 'createLockingSerial(insert): failed');
            cb(err);
        });
        q.once('end', function () {
            log.debug('createLockingSerial: done');
            cb();
        });
    });
}


function createTable(req, cb) {
    var bucket = req.bucket;
    var log = req.log;
    var pg = req.pg;
    var q;
    var sql = util.format(('CREATE TABLE IF NOT EXISTS %s (' +
                           '_id INTEGER DEFAULT nextval(\'%s_serial\'), ' +
                           '_txn_snap INTEGER, ' +
                           '_key TEXT PRIMARY KEY, ' +
                           '_value TEXT NOT NULL, ' +
                           '_etag CHAR(8) NOT NULL, ' +
                           '_mtime BIGINT DEFAULT CAST ' +
                           '(EXTRACT (EPOCH FROM NOW()) * 1000 AS BIGINT), ' +
                           '_vnode BIGINT ' +
                           '%s)'),
                          bucket.name,
                          bucket.name,
                          common.buildIndexString(bucket.index));

    cb = once(cb);

    log.debug({
        bucket: bucket.name,
        sql: sql
    }, 'createTable: entered');
    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'createTable: failed');
        cb(err);
    });

    q.once('end', function () {
        log.debug('createTable: done');
        cb();
    });
}


function calculateDiff(req, cb) {
    if (!req.version_changed) {
        // skip if version has not changed
        cb(null);
        return;
    }

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

    if (req.opts.no_reindex || !req.bucket.options.version
                            || !req.version_changed) {
        // skip if bucket is versionless or reindexing excluded or version has
        // not changed
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
                doCreateIndexes({
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
    if (!req.version_changed) {
        cb();
        return;
    }

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
    if (!req.version_changed || req.diff.del.length === 0) {
        cb();
        return;
    }

    cb = once(cb);

    var log = req.log;
    var pg = req.pg;
    const sql = 'ALTER TABLE $1 DROP COLUMN IF EXISTS $2';

    log.debug({
        bucket: req.bucket.name,
        del: req.diff.del.join(', ')
    }, 'dropColumns: entered');

    var _dropColumns = function _dropColumns(c, _cb) {
        _cb = once(_cb);
        var vals = [ req.bucket.name, c ];
        var q = pg.query(sql, vals);
        q.once('error', _cb);
        q.once('end', function () {
            _cb();
        });
    };

    vasync.forEachParallel({
        func: _dropColumns,
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
    if (!req.version_changed || req.diff.add.length === 0) {
        cb();
        return;
    }

    cb = once(cb);

    var log = req.log;
    var pg = req.pg;
    const sql = 'ALTER TABLE $1 ADD COLUMN IF NOT EXISTS $2 $3';

    log.debug({
        bucket: req.bucket.name,
        add: req.diff.add.join(', ')
    }, 'addColumns: entered');

    var _addColumns = function _addColumns(c, _cb) {
        _cb = once(_cb);
        var type = typeToPg(req.bucket.index[c].type);
        var vals = [ req.bucket.name, c, type ];
        log.debug({
            bucket: req.bucket.name,
            sql: str,
            vals: vals
        }, 'addColumns: adding column');
        var q = pg.query(sql, vals);
        q.once('error', _cb);
        q.once('end', function () {
            _cb();
        });
    };

    vasync.forEachParallel({
        func: _addColumns,
        inputs: req.diff.add
    }, function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'addColumns: %s', err ? 'failed' : 'done');
        cb(err);
    });
}


function doCreateIndexes(opts, cb) {
    var bucket = opts.bucket;
    var log = opts.log;
    var pg = opts.pg;

    var queries = opts.indexes.map(function (i) {
        var sql = util.format(('CREATE %s INDEX IF NOT EXISTS %s_%s_idx ' +
                               'ON %s USING %s (%s) ' +
                               'WHERE %s IS NOT NULL'),
                              (opts.unique ? 'UNIQUE' : ''), bucket, i.name,
                              bucket, i.type, i.name,
                              i.name);
        return (sql);
    });

    cb = once(cb);

    log.debug({bucket: bucket}, 'createIndexes: entered');
    vasync.forEachParallel({
        func: function createIndex(sql, _cb) {
            _cb = once(_cb);
            log.debug('createIndexes: running %s', sql);
            var q = pg.query(sql);
            q.once('error', function (err) {
                if (err) {
                    log.error({
                        err: err,
                        sql: sql
                    }, 'createIndex: failed');
                }
                _cb(err);
            });
            q.once('end', function () {
                _cb();
            });
        },
        inputs: queries
    }, cb);
}

function createIndexes(req, cb) {
    cb = once(cb);

    if (req.version_changed && req.diff.add.length === 0) {
        cb();
        return;
    }

    var idxs;
    if (req.version_changed) {
        var filterFun = function (k) {
                            return (!req.bucket.index[k].unique);
                        };
        idxs = req.diff.add.filter(filterFun);
    } else {
        var filterfun = function (k) {
                            return (!req.bucket.index[k].unique);
                        };
        idxs = Object.keys(req.bucket.index).filter(filterFun)
              .concat('_id', '_etag', '_mtime', '_vnode');

    }

    add = idxs.map(common.mapIndexType.bind(null, req.bucket.index));

    if (add.length === 0) {
        cb();
        return;
    }

    doCreateIndexes({
        bucket: req.bucket.name,
        log: req.log,
        pg: req.pg,
        indexes: add
    }, cb);
}


function createUniqueIndexes(req, cb) {
    cb = once(cb);

    if (!req.version_changed || req.diff.add.length === 0) {
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

    doCreateIndexes({
        bucket: req.bucket.name,
        log: req.log,
        pg: req.pg,
        unique: true,
        indexes: add
    }, cb);
}


function shootdownBucket(req, cb) {
    if (!req.version_changed) {
        cb();
        return;
    }

    common.shootdownBucket(req);
    cb();
}

///--- API

function createOrUpdate(options) {
    control.assertOptions(options);

    function _createOrUpdate(rpc) {
        const argsSchema = [
            { name: 'name', type: 'string' },
            { name: 'config', type: 'object' },
            { name: 'options', type: 'options' }
        ];

        const pipeline = [
            control.getPGHandleAndTransaction,
            common.validateBucket,
            ensureReindexProperty,
            loadBucket,
            createSequence,
            createLockingSerial,
            createTable,
            calculateDiff,
            ensureRowVer,
            updateConfig,
            dropColumns,
            addColumns,
            createIndexes,
            createUniqueIndexes,
            shootdownBucket
        ];

        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, argsSchema)) {
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
        }, 'createOrUpdateBucket: entered');

        control.handlerPipeline({
            req: req,
            funcs: pipeline
        });
    }

    return (_createOrUpdate);
}


///--- Exports

module.exports = {
    createOrUpdate: createOrUpdate
};
