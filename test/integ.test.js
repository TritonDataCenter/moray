// Copyright 2012 Joyent.  All rights reserved.

var uuid = require('node-uuid');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;



///--- Tests

before(function (cb) {
        this.bucket = 'moray_unit_test_' + uuid.v4().substr(0, 7);

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


test('MANTA-117 single quotes not being escaped', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        name: {
                                type: 'string',
                                unique: true
                        }
                }
        };
        var data = {
                name: uuid(),
                chain: [ {
                        name: 'A Task',
                        timeout: 30,
                        retry: 3,
                        body: function (job, cb) {
                                return cb(null);
                        }.toString()
                }],
                timeout: 180,
                onerror: [ {
                        name: 'Fallback task',
                        body: function (job, cb) {
                                return cb('Workflow error');
                        }.toString()
                }]
        };

        Object.keys(data).forEach(function (p) {
                if (typeof (data[p]) === 'object')
                        data[p] = JSON.stringify(data[p]);
        });

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        c.putObject(b, k, data, function (err3) {
                                t.ifError(err3);
                                t.end();
                        });
                });
        });
});


test('MANTA-328 numeric values in filters', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 123
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var ok = false;
                        var f = '(num=123)';
                        var req = c.findObjects(b, f);
                        req.once('error', function (err) {
                                t.ifError(err);
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                        req.once('record', function (obj) {
                                t.ok(obj);
                                t.equal(obj.bucket, b);
                                t.equal(obj.key, k);
                                t.deepEqual(obj.value, data);
                                t.ok(obj._id);
                                t.ok(obj._etag);
                                t.ok(obj._mtime);
                                ok = true;
                        });
                });
        });
});


test('MANTA-328 numeric values in filters <=', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 425
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var ok = false;
                        var f = '(num<=1024)';
                        var req = c.findObjects(b, f);
                        req.once('error', function (err) {
                                t.ifError(err);
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                        req.once('record', function (obj) {
                                t.ok(obj);
                                t.equal(obj.bucket, b);
                                t.equal(obj.key, k);
                                t.deepEqual(obj.value, data);
                                t.ok(obj._id);
                                t.ok(obj._etag);
                                t.ok(obj._mtime);
                                ok = true;
                        });
                });
        });
});


test('MANTA-328 numeric values in filters >=', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 425
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var ok = false;
                        var f = '(num>=81)';
                        var req = c.findObjects(b, f);
                        req.once('error', function (err) {
                                t.ifError(err);
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                        req.once('record', function (obj) {
                                t.ok(obj);
                                t.equal(obj.bucket, b);
                                t.equal(obj.key, k);
                                t.deepEqual(obj.value, data);
                                t.ok(obj._id);
                                t.ok(obj._etag);
                                t.ok(obj._mtime);
                                ok = true;
                        });
                });
        });
});


test('MANTA-170 bogus filter', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 425
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var f = '(num>81)';
                        var req = c.findObjects(b, f);
                        req.once('error', function (err) {
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(false);
                                t.end();
                        });
                });
        });
});


test('MANTA-680 boolean searches', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        b: {
                                type: 'boolean'
                        }
                }
        };
        var data = {
                b: true
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var f = '(b=true)';
                        var req = c.findObjects(b, f);
                        var ok = false;
                        req.once('record', function () {
                                ok = true;
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                });
        });
});
