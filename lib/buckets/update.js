// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var uuid = require('node-uuid');
var vasync = require('vasync');

var common = require('./common');
require('../errors');


///--- Globals

var sprintf = util.format;



///--- Handlers

function loadIndexes(req, cb) {
        var b = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('SELECT index FROM buckets_config WHERE name=\'%s\'',
                          b.name);

        log.debug({
                bucket: b.name
        }, 'loadBucket: entered');
        pg.query(sql, function (err, result) {
                if (err) {
                        log.debug({
                                bucket: b.name,
                                err: err
                        }, 'loadIndexes: failed');
                        cb(err);
                } else if (result.rows.length > 0) {
                        req.index = JSON.parse(result.rows[0].index);
                        log.debug({
                                bucket: b.name,
                                oldIndex: req.index
                        }, 'loadIndexes: done');
                        cb();
                } else {
                        cb(new BucketNotFoundError(req.bucket.name));
                }
        });
}


function updateConfig(req, cb) {
        var bucket = req.bucket;
        var log = req.log;
        var pg = req.pg;
        var sql = 'UPDATE buckets_config ' +
                'SET index=$1, pre=$2, post=$3 ' +
                'WHERE name=$4';
        var values = [JSON.stringify(bucket.index)];
        values.push(JSON.stringify((bucket.pre || []).map(function (f) {
                return (f.toString());
        })));
        values.push(JSON.stringify((bucket.post || []).map(function (f) {
                return (f.toString());
        })));
        values.push(bucket.name);

        log.debug({
                bucket: bucket.name,
                values: values
        }, 'updateConfig: entered');
        pg.query(sql, values, function (err) {
                if (err) {
                        log.debug({
                                bucket: bucket.name,
                                err: err
                        }, 'updateConfig: failed');
                        cb(err);
                } else {
                        log.debug({
                                bucket: bucket.name
                        }, 'updateConfig: done');
                        cb();
                }
        });
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


function dropColumns(req, cb) {
        if (req.diff.del.length === 0)
                return (cb());

        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('ALTER TABLE %s DROP COLUMN ', req.bucket.name);

        log.debug({
                bucket: req.bucket.name,
                del: req.diff.del.join(', ')
        }, 'dropColumns: entered');
        vasync.forEachParallel({
                func: function _drop(c, _cb) {
                        pg.query(sql + c, function (err) {
                                _cb(err);
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

        return (undefined);
}


function addColumns(req, cb) {
        if (req.diff.add.length === 0)
                return (cb());

        var log = req.log;
        var pg = req.pg;
        var sql = sprintf('ALTER TABLE %s ADD COLUMN ', req.bucket.name);

        log.debug({
                bucket: req.bucket.name,
                add: req.diff.add.join(', ')
        }, 'addColumns: entered');
        vasync.forEachParallel({
                func: function _drop(c, _cb) {
                        var str = sql + c +
                                ' ' + common.typeToPg(req.bucket.index[c].type);
                        log.debug({
                                bucket: req.bucket.name,
                                sql: str
                        }, 'addColumns: adding column');
                        pg.query(str, function (err) {
                                _cb(err);
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

        return (undefined);
}


function createIndexes(req, cb) {
        if (req.diff.add.length === 0)
                return (cb());

        var add = req.diff.add.filter(function (k) {
                return (!req.bucket.index[k].unique);
        });

        if (add.length === 0)
                return (cb());

        common.createIndexes({
                bucket: req.bucket.name,
                log: req.log,
                pg: req.pg,
                indexes: add
        }, cb);

        return (undefined);
}


function createUniqueIndexes(req, cb) {
        if (req.diff.add.length === 0)
                return (cb());

        var add = req.diff.add.filter(function (k) {
                return (req.bucket.index[k].unique);
        });

        if (add.length === 0)
                return (cb());

        common.createIndexes({
                bucket: req.bucket.name,
                log: req.log,
                pg: req.pg,
                unique: true,
                indexes: add
        }, cb);

        return (undefined);
}


function update(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.object(options.manatee, 'options.manatee');

        var manatee = options.manatee;

        function _update(name, cfg, opts, res) {
                var log = options.log.child({
                        req_id: opts.req_id || uuid.v1()
                });

                log.debug({
                        bucket: name,
                        cfg: cfg,
                        opts: opts
                }, 'updateBucket: entered');

                manatee.start(function (startErr, pg) {
                        if (startErr) {
                                log.debug(startErr,
                                          'updateBucket: no DB handle');
                                return (res.end(startErr));
                        }

                        log.debug({
                                pg: pg
                        }, 'updateBucket: transaction started');

                        vasync.pipeline({
                                funcs: [
                                        common.validateBucket,
                                        loadIndexes,
                                        updateConfig,
                                        calculateDiff,
                                        dropColumns,
                                        addColumns,
                                        createIndexes,
                                        createUniqueIndexes,
                                        common.commit
                                ],
                                arg: {
                                        bucket: {
                                                name: name,
                                                index: cfg.index || {},
                                                pre: cfg.pre || [],
                                                post: cfg.post || []
                                        },
                                        log: log,
                                        pg: pg,
                                        manatee: manatee
                                }
                        }, function (err) {
                                if (err) {
                                        log.warn(err, 'updateBucket: failed');
                                        pg.rollback(function () {
                                                res.end(err);
                                        });
                                } else {
                                        log.debug('updateBucket: done');
                                        res.end();
                                }
                        });

                        return (undefined);
                });
        }

        return (_update);
}



///--- Exports

module.exports = {
        update: update
};
