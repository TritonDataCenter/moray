/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var clone = require('clone');
var tape = require('tape');
var uuid = require('libuuid').create;

var helper = require('./helper.js');



///--- Globals

var FULL_CFG = {
    index: {
        str: {
            type: 'string'
        },
        str_u: {
            type: 'string',
            unique: true
        },
        num: {
            type: 'number'
        },
        num_u: {
            type: 'number',
            unique: true
        },
        bool: {
            type: 'boolean'
        },
        bool_u: {
            type: 'boolean',
            unique: true
        },
        ip: {
            type: 'ip'
        },
        ip_u: {
            type: 'ip',
            unique: true
        },
        subnet: {
            type: 'subnet'
        },
        subnet_u: {
            type: 'subnet',
            unique: true
        }
    },
    pre: [function onePre(req, cb) { cb(); }],
    post: [function onePost(req, cb) { cb(); }],
    options: {}
};

var c; // client
var server;
var b; // bucket

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
        b = 'moray_unit_test_' + uuid().substr(0, 7);
        helper.createServer(function (s) {
            server = s;
            c = helper.createClient();
            c.on('connect', t.end.bind(t));
        });
    });

    tape.test(name + ' - main', function (t) {
        setup(t);
    });

    tape.test(name + ' - teardown', function (t) {
        // May or may not exist, just blindly ignore
        c.delBucket(b, function () {
            c.once('close', function () {
                helper.cleanupServer(server, function () {
                    t.pass('closed');
                    t.end();
                });
            });
            c.close();
        });
    });
}


///--- Helpers

function assertBucket(t, bucket, cfg) {
    t.ok(bucket);
    if (!bucket)
        return (undefined);
    t.equal(bucket.name, b);
    t.ok(bucket.mtime instanceof Date);
    t.deepEqual(bucket.index, (cfg.index || {}));
    t.ok(Array.isArray(bucket.pre));
    t.ok(Array.isArray(bucket.post));
    t.equal(bucket.pre.length, (cfg.pre || []).length);
    t.equal(bucket.post.length, (cfg.post || []).length);

    if (bucket.pre.length !== (cfg.pre || []).length ||
        bucket.post.length !== (cfg.post || []).length)
        return (undefined);
    var i;
    for (i = 0; i < bucket.pre.length; i++)
        t.equal(bucket.pre[i].toString(), cfg.pre[i].toString());
    for (i = 0; i < bucket.post.length; i++)
        t.equal(bucket.post[i].toString(), cfg.post[i].toString());

    return (undefined);
}


///--- tests


test('create bucket stock config', function (t) {
    c.createBucket(b, {}, function (err) {
        t.ifError(err);
        c.getBucket(b, function (err2, bucket) {
            t.ifError(err2);
            assertBucket(t, bucket, {});
            c.listBuckets(function (err3, buckets) {
                t.ifError(err3);
                t.ok(buckets);
                t.ok(buckets.length);
                t.end();
            });
        });
    });
});


test('create bucket loaded', function (t) {
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        c.getBucket(b, function (err2, bucket) {
            t.ifError(err2);
            assertBucket(t, bucket, FULL_CFG);
            t.end();
        });
    });
});


test('update bucket', function (t) {
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        var cfg = clone(FULL_CFG);
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.end();
            });
        });
    });
});


test('update bucket (versioned ok 0->1)', function (t) {
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        var cfg = clone(FULL_CFG);
        cfg.options.version = 1;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.end();
            });
        });
    });
});


test('update bucket (versioned ok 1->2)', function (t) {
    var cfg = clone(FULL_CFG);

    cfg.options.version = 1;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = clone(FULL_CFG);
        cfg.options.version = 2;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.end();
            });
        });
    });
});


test('update bucket (reindex tracked)', function (t) {
    var cfg = clone(FULL_CFG);

    cfg.options.version = 1;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = clone(FULL_CFG);
        cfg.options.version = 2;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.ok(bucket.reindex_active);
                t.ok(bucket.reindex_active['2']);
                t.end();
            });
        });
    });
});


test('update bucket (reindex disabled)', function (t) {
    var cfg = clone(FULL_CFG);

    cfg.options.version = 1;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = clone(FULL_CFG);
        cfg.options.version = 2;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        var opts = {
            no_reindex: true
        };
        c.updateBucket(b, cfg, opts, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.notOk(bucket.reindex_active);
                t.end();
            });
        });
    });
});


test('update bucket (null version, reindex disabled)', function (t) {
    var cfg = clone(FULL_CFG);

    cfg.options.version = 0;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = clone(FULL_CFG);
        cfg.options.version = 0;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.notOk(bucket.reindex_active);
                t.end();
            });
        });
    });
});


test('update bucket (versioned not ok 1 -> 0)', function (t) {
    var cfg = clone(FULL_CFG);
    cfg.options.version = 1;

    c.createBucket(b, cfg, function (err) {
        t.ifError(err);

        cfg = clone(FULL_CFG);
        cfg.options.version = 0;

        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });

        c.updateBucket(b, cfg, function (err2) {
            t.ok(err2);
            if (err2) {
                t.equal(err2.name, 'BucketVersionError');
                t.ok(err2.message);
            }
            t.end();
        });
    });
});


test('update bucket (versioned not ok 2 -> 1)', function (t) {
    var cfg = clone(FULL_CFG);
    cfg.options.version = 2;

    c.createBucket(b, cfg, function (err) {
        t.ifError(err);

        cfg = clone(FULL_CFG);
        cfg.options.version = 1;

        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });

        c.updateBucket(b, cfg, function (err2) {
            t.ok(err2);
            if (err2) {
                t.equal(err2.name, 'BucketVersionError');
                t.ok(err2.message);
            }
            t.end();
        });
    });
});


test('create bucket bad index type', function (t) {
    c.createBucket(b, {index: {foo: 'foo'}}, function (err) {
        t.ok(err);
        t.equal(err.name, 'InvalidBucketConfigError');
        t.ok(err.message);
        t.end();
    });
});


test('create bucket triggers not function', function (t) {
    c.createBucket(b, {pre: ['foo']}, function (err) {
        t.ok(err);
        t.equal(err.name, 'NotFunctionError');
        t.ok(err.message);
        t.end();
    });
});


test('get bucket 404', function (t) {
    c.getBucket(uuid().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'BucketNotFoundError');
        t.ok(err.message);
        t.end();
    });
});


test('delete missing bucket', function (t) {
    c.delBucket(uuid().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'BucketNotFoundError');
        t.ok(err.message);
        t.end();
    });
});
