/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var libuuid = require('libuuid');
var tape = require('tape');
var once = require('once');
var vasync = require('vasync');

var helper = require('./helper.js');



///--- Globals

var c; // client
var server;
var b; // bucket

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
        b = 'moray_unit_test_' + libuuid.create().substr(0, 7);
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
            c.on('close', function () {
                helper.cleanupServer(server, function () {
                    t.pass('closed');
                    t.end();
                });
            });
            c.close();
        });
    });
}


///--- Tests


test('MANTA-117 single quotes not being escaped', function (t) {
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
                    t.equal(obj.value.foo, '8');
                    t.equal(obj.value.bar, '8');
                    t.equal(obj.value.baz, '8');
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


test('MANTA-1726 batch+update+limit', function (t) {

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
            function batchUpdateMany(_, cb) {
                cb = once(cb);

                c.batch([
                    {
                        operation: 'update',
                        bucket: b,
                        filter: 'n>=0',
                        options: {
                            limit: 50
                        },
                        fields: {
                            n: 10000
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


test('MORAY-131 case insensitive match', function (t) {
    var k = libuuid.create();
    var cfg = {
        index: {
            str: {
                type: 'string'
            }
        }
    };
    var data = {
        str: 'MaRk'
    };

    c.putBucket(b, cfg, function (err1) {
        t.ifError(err1);
        c.putObject(b, k, data, function (err2) {
            t.ifError(err2);
            var f = '(str:caseIgnoreMatch:=mark)';
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


test('MORAY-131 case insensitive substrings match', function (t) {
    var k = libuuid.create();
    var cfg = {
        index: {
            str: {
                type: 'string'
            }
        }
    };
    var data = {
        str: 'MaRk'
    };

    c.putBucket(b, cfg, function (err1) {
        t.ifError(err1);
        c.putObject(b, k, data, function (err2) {
            t.ifError(err2);
            var f = '(str:caseIgnoreSubstringsMatch:=m*r*)';
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
