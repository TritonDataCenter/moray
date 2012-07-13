// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var restify = require('restify');
var vasync = require('vasync');

var common = require('./common');
require('./errors');



///--- Globals

var sprintf = util.format;
var INDEX_TYPES = {
        string: true,
        number: true,
        'boolean': true
};

// Postgres rules:
// start with a letter, everything else is alphum or '_', and must be
// <= 63 characters in length
var BUCKET_NAME_RE = /^[a-zA-Z]\w{0,62}$/;

var RESERVED_BUCKETS = ['moray', 'search'];




///--- Helpers

function diffSchema(prev, next) {
        assert.object(prev, 'previous');
        assert.object(next, 'next');

        var diff = {
                add: [],
                del: [],
                mod: []
        };

        Object.keys(next).forEach(function (k) {
                if (prev[k] === undefined) {
                        diff.add.push(k);
                } else if (!deepEqual(next[k], prev[k])) {
                        diff.mod.push(k);
                }
        });

        Object.keys(prev).forEach(function (k) {
                if (!next[k])
                        diff.del.push(k);
        });

        return (diff);
}


function indexString(schema) {
        assert.object(schema, 'schema');

        var str = '';
        Object.keys(schema).forEach(function (k) {
                str += ',\n        ' + k + ' ' + typeToPg(schema[k].type);
                if (schema[k].unique)
                        str += ' UNIQUE';
        });

        return (str);
}


function typeToPg(type) {
        assert.string(type, 'type');

        var pgType;

        switch (type) {
        case 'number':
                pgType = 'NUMERIC';
                break;
        case 'boolean':
                pgType = 'BOOLEAN';
                break;
        case 'string':
                pgType = 'TEXT';
                break;
        default:
                throw new InvalidIndexTypeError(type);
        }

        return (pgType);
}


function validateIndexes(schema) {
        var i, j, k, k2, keys, msg, sub, subKeys;

        console.log(schema)
        keys = Object.keys(schema);
        for (i = 0; i < keys.length; i++) {
                k = keys[i];
                if (typeof (k) !== 'string')
                        return (new InvalidIndexError('keys must be strings'));
                if (typeof (schema[k]) !== 'object')
                        return (new InvalidIndexError('values must be objects'));

                sub = schema[k];
                subKeys = Object.keys(sub);
                for (j = 0; j < subKeys.length; j++) {
                        k2 = subKeys[j];
                        switch (k2) {
                        case 'type':
                                if (sub[k2] !== 'string' &&
                                    sub[k2] !== 'number' &&
                                    sub[k2] !== 'boolean') {
                                        msg = k + '.type is invalid';
                                        return (new InvalidIndexError(msg));
                                }
                                break;
                        case 'unique':
                                if (typeof (sub[k2]) !== 'boolean') {
                                        msg = k + '.unique must be boolean';
                                        return (new InvalidIndexError(msg));
                                }
                                break;
                        default:
                                msg = k + '.' + k2 + ' is invalid';
                                return (new InvalidIndexError(msg));
                        }
                }
        }

        return (null);
}



///--- Handlers

//-- PUT Handlers

function validatePutBucketRequest(req, res, next) {
        var i, fn, err;
        var post = [];
        var pre = [];

        req.log.debug('validatePutBucketRequest: entered');

        req.params.index = req.params.index || {};
        req.params.post = req.params.post || [];
        req.params.pre = req.params.pre || [];

        req.params.post = req.params.post.map(function (p) {
                return (eval('fn = ' + p));
        });

        req.params.pre = req.params.pre.map(function (p) {
                return (eval('fn = ' + p));
        });

        // Make JSLint shutup
        if (fn)
                fn = null;
        // End Make JSLint shutup

        if (!BUCKET_NAME_RE.test(req.params.bucket))
                return (next(new InvalidBucketNameError(req.params.bucket)));

        if (RESERVED_BUCKETS.indexOf(req.params.bucket) !== -1)
                return (next(new InvalidBucketNameError(bucket)));

        if (typeof (req.params.index) !== 'object' ||
            Array.isArray(req.params.index)) {
                return (next(new InvalidIndexError('index is invalid')));
        }

        if (!Array.isArray(req.params.post))
                return (next(new NotFunctionError('post')));

        try {
                assert.arrayOfFunc(req.params.post);
        } catch (e) {
                req.log.debug(e, 'validation of req.params.post failed');
                return (next(new NotFunctionError('post')));
        }

        if (!Array.isArray(req.params.pre))
                return (next(new NotFunctionError('pre')));

        try {
                assert.arrayOfFunc(req.params.pre);
        } catch (e) {
                req.log.debug(e, 'validation of req.params.pre failed');
                return (next(new NotFunctionError('pre')));
        }

        req.log.debug('validatePutBucketRequest: checks ok; inspecting index');
        return (next(validateIndexes(req.params.index)));
}


function insertBucketConfig(req, res, next) {
        if (req.bucket)
                return (next());

        var log = req.log;
        var opts = {
                client: req.client,
                sql: 'INSERT INTO buckets_config (name, index, pre, post)' +
                        ' VALUES ($1, $2, $3, $4)',
                values: [
                        req.params.bucket,
                        JSON.stringify(req.params.index),
                        JSON.stringify(req.params.pre),
                        JSON.stringify(req.params.post)
                ]
        };

        log.debug({
                bucket: req.params.bucket,
                record: opts.values
        }, 'insertBucketConfig: entered');
        req.pg.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'insertBucketConfig: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
        return (undefined);
}


function updateBucketConfig(req, res, next) {
        if (!req.bucket)
                return (next());

        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('UPDATE buckets_config SET index=\'%s\',' +
                             ' pre=$moray$%s$moray$,' +
                             'post=$moray$%s$moray$ WHERE name=\'%s\'',
                             JSON.stringify(req.params.index),
                             JSON.stringify(req.params.pre),
                             JSON.stringify(req.params.post),
                             req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket
        }, 'updateBucketConfig: entered');
        req.pg.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'updateBucketConfig: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
        return (undefined);
}


function alterEntryTable(req, res, next) {
        if (!req.bucket)
                return (next());

        var bucket = req.params.bucket;
        var log = req.log;
        var prev = req.bucket.index;
        var index = req.params.index;
        var diff = diffSchema(prev, index);
        var tasks;
        if (diff.mod.length > 0)
                return (next(new SchemaChangeError(bucket, diff.mod.join())));

        log.debug({diff: diff}, 'alterEntryTable: entered');

        // Build up a vasync series of DROP then ADD columns
        tasks = diff.del.map(function (column) {
                var sql = sprintf('ALTER TABLE %s_entry DROP COLUMN %s',
                                  bucket, column);
                return (sql);
        }).concat(diff.add.map(function (column) {
                var type = typeToPg(index[column].type);
                var sql = sprintf('ALTER TABLE %s_entry ADD COLUMN %s %s',
                                  bucket, column, type);
                if (index[column].unique)
                        sql += ' UNIQUE';
                return (sql);
        }));

        log.debug('alterEntryTable: issuing SQL commands');
        vasync.forEachParallel({
                func: function update(sql, cb) {
                        req.pg.query({
                                client: req.client,
                                sql: sql
                        }, cb);
                },
                inputs: tasks
        }, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: bucket,
                                err: err
                        }, 'alterEntryTable: %s', (err ? 'failed' : 'done'));
                }
                req.diff = diff;
                next(err);
        });
        return (undefined);
}


function createEntryTable(req, res, next) {
        if (req.bucket)
                return (next());

        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('CREATE TABLE %s_entry (' +
                             '_id SERIAL, ' +
                             '_key TEXT PRIMARY KEY, ' +
                             '_value TEXT NOT NULL, ' +
                             '_etag CHAR(32) NOT NULL, ' +
                             '_mtime TIMESTAMP NOT NULL ' +
                             'DEFAULT CURRENT_TIMESTAMP%s)',
                             req.params.bucket,
                             indexString(req.params.index))
        };

        log.debug({
                bucket: req.params.bucket
        }, 'createEntryTable: entered');
        req.pg.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'createEntryTable: %s', (err ? 'failed' : 'done'));
                }
                // Stub out the diff so createIndexes will work correctly
                req.diff = {
                        add: Object.keys(req.params.index)
                };
                next(err);
        });
        return (undefined);
}


function createIndexes(req, res, next) {
        // We just blindly stomp over old indexes. Postgres will throw,
        // and we ignore

        var bucket = req.params.bucket;
        var schema = req.params.index;
        var sql = req.diff.add.filter(function (k) {
                return (!schema[k].unique);
        }).map(function (k) {
                var sql = sprintf('CREATE INDEX %s_entry_%s_idx ' +
                                  'ON %s_entry(%s) ' +
                                  'WHERE %s IS NOT NULL',
                                  bucket, k, bucket, k, k);
                return (sql);
        });

        vasync.forEachParallel({
                func: function createIndex(sql, cb) {
                        var opts = {
                                client: req.client,
                                sql: sql
                        };
                        req.pg.query(opts, function (err) {
                                if (err) {
                                        req.log.warn({
                                                err: err,
                                                sql: sql
                                        }, 'createIndex: failed');
                                }
                                cb();
                        });
                },
                inputs: sql
        }, next);
}


function putDone(req, res, next) {
        res.send(204);
        next();
}


//-- GET Handlers

function getDone(req, res, next) {
        if (req.bucket) {
                res.send(200, req.bucket);
                next();
        } else {
                next(new BucketNotFoundError(req.params.bucket));
        }
}

//-- DELETE Handlers

function dropTable(req, res, next) {
        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('DROP TABLE %s_entry', req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket,
        }, 'dropTable: entered');
        req.pg.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'dropTable: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
}


function deleteBucketConfig(req, res, next) {
        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('DELETE FROM buckets_config WHERE name = \'%s\'',
                             req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket,
        }, 'deleteBucketConfig: entered');
        req.pg.query(opts, function (err) {
                if (log.debug()) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'deleteBucketConfig: %s', (err ? 'failed' : 'done'));
                }
                next(err);
        });
}


function delDone(req, res, next) {
        res.send(204);
        next();
}



///--- Exports

module.exports = {
        put: function put() {
                var chain = [
                        common.resume,
                        restify.bodyParser(),
                        validatePutBucketRequest,
                        common.startTransaction,
                        common.loadBucket,
                        insertBucketConfig,
                        updateBucketConfig,
                        createEntryTable,
                        alterEntryTable,
                        createIndexes,
                        common.commitTransaction,
                        putDone
                ];
                return (chain);
        },

        get: function get() {
                var chain = [
                        common.startTransaction,
                        common.loadBucket,
                        common.rollbackTransaction,
                        getDone
                ];
                return (chain);
        },

        del: function del() {
                var chain = [
                        common.startTransaction,
                        common.loadBucket,
                        dropTable,
                        deleteBucketConfig,
                        common.commitTransaction,
                        delDone
                ];
                return (chain);
        }
};


// function paramToNumber(name, param, max) {
//     try {
//         var n = parseInt(param, 10);
//         if (max)
//             n = Math.min(n, max);

//         return n;
//     } catch (e) {
//         throw new InvalidArgumentError('%s was not a valid number', name);
//     }
// }


// function createMarker(req, opts) {
//     var iv = req.config.marker.iv;
//     var key = req.config.marker.key;
//     var marker = '';

//     var aes = crypto.createCipheriv('aes-128-cbc', key, iv);
//     marker += aes.update(new Date().getTime() + '::', 'utf8', 'base64');
//     marker += aes.update(JSON.stringify(opts), 'utf8', 'base64');
//     marker += aes.final('base64');

//     return marker;
// }


// function parseMarker(req, marker) {
//     try {
//         var dec = '';
//         var iv = req.config.marker.iv;
//         var key = req.config.marker.key;

//         var aes = crypto.createDecipheriv('aes-128-cbc', key, iv);
//         dec += aes.update(marker, 'base64', 'utf8');
//         dec += aes.final('utf8');
//         return JSON.parse(dec.split('::')[1]);
//     } catch (e) {
//         req.log.debug({err: e}, 'Error parsing marker');
//         throw new InvalidArgumentError('marker is invalid');
//     }
// }


// function paginationOptions(req) {
//     var opts;

//     if (req.params.marker) {
//         opts = parseMarker(req, req.params.marker);
//     } else {
//         opts = {
//             limit: 1000,
//             offset: 0
//         };
//         if (req.params.limit)
//             opts.limit = paramToNumber('limit', req.params.limit, 1000);
//         if (req.params.prefix)
//             opts.prefix = req.params.prefix;
//     }

//     return opts;
// }



// ///--- Routes

// function list(req, res, next) {
//     var db = req.bucketManager;
//     var log = req.log;

//     log.debug('ListBuckets entered');
//     return db.list(function (err, buckets) {
//         if (err)
//             return next(err);

//         Object.keys(buckets).forEach(function (k) {
//             buckets[k].pre = buckets[k].pre.map(function (p) {
//                 return p.toString();
//             });
//             buckets[k].post = buckets[k].post.map(function (p) {
//                 return p.toString();
//             });
//         });

//         log.debug({buckets: buckets}, 'ListBuckets done');
//         res.send(buckets);
//         return next();
//     });
// }


// function put(req, res, next) {
//         var db = req.bucketManager;
//         var fn;
//         var log = req.log;
//         var post = [];
//         var pre = [];
//         var schema = req.params.schema || {};

//         (req.params.pre || []).forEach(function (p) {
//                 pre.push(eval('fn = ' + p));
//         });

//         (req.params.post || []).forEach(function (p) {
//                 post.push(eval('fn = ' + p));
//         });
//         if (fn) {
//                 fn = null;
//         } // make javascriptlint shutup

//     log.debug({
//         bucket: req.params.bucket,
//         schema: schema,
//         pre: pre,
//         post: post
//     }, 'PutBucket entered');

//     var opts = {
//         schema: schema,
//         pre: pre,
//         post: post
//     };
//     return db.put(req.params.bucket, opts, function (err) {
//         if (err)
//             return next(err);

//         log.debug({bucket: req.params.bucket}, 'PutBucket done');
//         res.send(204);
//         return next();
//     });
// }


// function getBucketSchema(req, res, next) {
//     var db = req.bucketManager;
//     var log = req.log;

//     req.bucket = {};
//     if (req.params.schema === false || req.params.schema === 'false')
//         return next();

//     log.debug({bucket: req.params.bucket}, 'GetBucketSchema entered');
//     return db.get(req.params.bucket, function (err, bucket) {
//         if (err)
//             return next(err);

//         req.bucket = bucket;
//         req.bucket.pre = req.bucket.pre.map(function (p) {
//             return p.toString();
//         });
//         req.bucket.post = req.bucket.post.map(function (p) {
//             return p.toString();
//         });

//         return next();
//     });
// }


// function listKeys(req, res, next) {
//     assert.ok(req.bucket);

//     if (req.params.keys === false || req.params.keys === 'false')
//         return next();

//     var db = req.bucketManager;
//     var opts = paginationOptions(req);

//     return db.keys(req.params.bucket, opts, function (err, keys) {
//         if (err)
//             return next(err);

//         req.bucket.keys = {};
//         res.header('x-total-keys', keys.total);

//         keys.keys.forEach(function (k) {
//             req.bucket.keys[k.key] = {
//                 etag: k.etag,
//                 mtime: k.mtime
//             };
//         });

//         if ((keys.keys.length + opts.offset) < keys.total) {
//             opts.offset = opts.offset + opts.limit;
//             var marker = createMarker(req, opts);
//             res.link(sprintf('/%s?marker=%s',
//                              req.params.bucket,
//                              encodeURIComponent(marker)),
//                      'next');
//         }

//         return next();
//     });
// }


// function getBucketDone(req, res, next) {
//     req.log.debug({
//         bucket: req.params.bucket,
//         body: req.bucket
//     }, 'GetBucket done');
//     res.send(req.bucket);
//     return next();
// }


// function del(req, res, next) {
//     var db = req.bucketManager;
//     var log = req.log;

//     log.debug({bucket: req.params.bucket}, 'DeleteBucket entered');
//     return db.del(req.params.bucket, function (err) {
//         if (err)
//             return next(err);

//         log.debug({bucket: req.params.bucket}, 'DeleteBucket done');
//         res.send(204);
//         return next();
//     });
// }



// ///--- Exports

// module.exports = {
//     mount: function mount(server) {
//         args.assertArgument(server, 'object', server);

//         server.get({path: '/', name: 'ListBuckets'}, list);
//         server.head({path: '/', name: 'ListBuckets'}, list);

//         var bodyParser = restify.bodyParser();
//         server.put({path: '/:bucket', name: 'PutBucket'}, bodyParser, put);

//         server.get({path: '/:bucket', name: 'GetBucket'},
//                    getBucketSchema, listKeys, getBucketDone);
//         server.head({path: '/:bucket', name: 'GetBucket'},
//                     getBucketSchema, listKeys, getBucketDone);

//         server.del({path: '/:bucket', name: 'DeleteBucket'}, del);

//         return server;
//     }
// };
