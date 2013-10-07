// Copyright 2013 Joyent.  All rights reserved.

var libuuid = require('libuuid');
var once = require('once');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;



///--- Tests

before(function (cb) {
    this.bucket = 'moray_unit_test_' + libuuid.create().substr(0, 7);

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
    var k = libuuid.create();
    var cfg = {
        index: {
            name: {
                type: 'string',
                unique: true
            }
        }
    };
    var data = {
        name: libuuid.create(),
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
    var k = libuuid.create();
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
    var k = libuuid.create();
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
    var k = libuuid.create();
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
    var k = libuuid.create();
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
    var k = libuuid.create();
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


test('some marlin query', function (t) {
    var b = this.bucket;
    var c = this.client;
    var cfg = {
        index: {
            foo: {
                type: 'string'
            },
            bar: {
                type: 'string'
            },
            baz: {
                type: 'string'
            }
        }
    };
    var found = false;

    vasync.pipeline({
        funcs: [
            function bucket(_, cb) {
                c.putBucket(b, cfg, cb);
            },
            function objects(_, cb) {
                cb = once(cb);

                var done = 0;
                function _cb(err) {
                    if (err) {
                        cb(err);
                        return;
                    }
                    if (++done === 10)
                        cb();
                }
                for (var i = 0; i < 10; i++) {
                    var data = {
                        foo: '' + i,
                        bar: '' + i,
                        baz: '' + i
                    };
                    c.putObject(b, libuuid.create(), data, _cb);
                }
            },
            function find(_, cb) {
                cb = once(cb);
                var f = '(&(!(|(foo=0)(foo=1)))(bar=8)(baz=8))';
                var req = c.findObjects(b, f);
                req.once('error', cb);
                req.once('record', function (obj) {
                    t.ok(obj);
                    t.equal(obj.value.foo, 8);
                    t.equal(obj.value.bar, 8);
                    t.equal(obj.value.baz, 8);
                    found = true;
                });
                req.once('end', cb);
            }
        ],
        arg: null
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('MANTA-1726 batch+deleteMany+limit', function (t) {
    var b = this.bucket;
    var c = this.client;

    vasync.pipeline({
        funcs: [
            function bucket(_, cb) {
                var cfg = {
                    index: {
                        n: {
                            type: 'number'
                        }
                    }
                };
                c.putBucket(b, cfg, once(cb));
            },
            function writeObjects(_, cb) {
                cb = once(cb);

                var done = 0;
                for (var i = 0; i < 100; i++) {
                    c.putObject(b, libuuid.create(), {n: i}, function (err) {
                        if (err) {
                            cb(err);
                        } else if (++done === 100) {
                            cb();
                        }
                    });
                }
            },
            function batchDeleteMany(_, cb) {
                cb = once(cb);

                c.batch([
                    {
                        operation: 'deleteMany',
                        bucket: b,
                        filter: 'n>=0',
                        options: {
                            limit: 50
                        }
                    }
                ], function (err, meta) {
                    if (err) {
                        cb(err);
                    } else {
                        t.ok(meta);
                        meta = meta || {};
                        t.ok((meta || {}).etags);
                        meta.etags = meta.etags || [];
                        t.ok(meta.etags.length);
                        if (meta.etags.length)
                            t.equal(meta.etags[0].count, 50);
                        cb();
                    }
                });
            }
        ],
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});
