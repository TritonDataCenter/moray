// Copyright 2012 Joyent.  All rights reserved.

var clone = require('clone');
var uuid = require('node-uuid');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

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
                }
        },
        pre: [function onePre(req, cb) { cb(); }],
        post: [function onePost(req, cb) { cb(); }],
        options: {}
};



///--- Helpers

function assertBucket(name, t, bucket, cfg) {
        t.ok(bucket);
        if (!bucket)
                return (undefined);
        t.equal(bucket.name, name);
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

before(function (cb) {
        this.bucket = 'moray_unit_test_' + uuid.v4().substr(0, 7);
        this.assertBucket = assertBucket.bind(null, this.bucket);

        this.client = helper.createClient();
        this.client.on('connect', cb);

});

after(function (cb) {
        var self = this;
        // May or may not exist, just blindly ignore
        this.client.delBucket(this.bucket, function () {
                self.client.close();
                cb();
        });
});


test('create bucket stock config', function (t) {
        var b = this.bucket;
        var c = this.client;
        var self = this;

        c.createBucket(b, {}, function (err) {
                t.ifError(err);
                c.getBucket(b, function (err2, bucket) {
                        t.ifError(err2);
                        self.assertBucket(t, bucket, {});
                        t.end();
                });
        });
});


test('create bucket loaded', function (t) {
        var b = this.bucket;
        var c = this.client;
        var self = this;

        c.createBucket(b, FULL_CFG, function (err) {
                t.ifError(err);
                c.getBucket(b, function (err2, bucket) {
                        t.ifError(err2);
                        self.assertBucket(t, bucket, FULL_CFG);
                        t.end();
                });
        });
});


test('update bucket', function (t) {
        var b = this.bucket;
        var c = this.client;
        var self = this;

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
                                self.assertBucket(t, bucket, cfg);
                                t.end();
                        });
                });
        });
});


test('update bucket (versioned ok 0->1)', function (t) {
        var b = this.bucket;
        var c = this.client;
        var self = this;

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
                                self.assertBucket(t, bucket, cfg);
                                t.end();
                        });
                });
        });
});


test('update bucket (versioned ok 1->2)', function (t) {
        var b = this.bucket;
        var c = this.client;
        var cfg = clone(FULL_CFG);
        var self = this;

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
                                self.assertBucket(t, bucket, cfg);
                                t.end();
                        });
                });
        });
});


test('update bucket (versioned not ok 1 -> 0)', function (t) {
        var b = this.bucket;
        var c = this.client;
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
        var b = this.bucket;
        var c = this.client;
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
        var b = this.bucket;
        var c = this.client;
        c.createBucket(b, {index: {foo: 'foo'}}, function (err) {
                t.ok(err);
                t.equal(err.name, 'InvalidBucketConfigError');
                t.ok(err.message);
                t.end();
        });
});


test('create bucket triggers not function', function (t) {
        var b = this.bucket;
        var c = this.client;
        c.createBucket(b, {pre: ['foo']}, function (err) {
                t.ok(err);
                t.equal(err.name, 'NotFunctionError');
                t.ok(err.message);
                t.end();
        });
});


test('get bucket 404', function (t) {
        var c = this.client;
        c.getBucket(uuid.v4().substr(0, 7), function (err) {
                t.ok(err);
                t.equal(err.name, 'BucketNotFoundError');
                t.ok(err.message);
                t.end();
        });
});
