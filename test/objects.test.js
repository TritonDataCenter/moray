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
var once = require('once');
var libuuid = require('libuuid');
var vasync = require('vasync');
var util = require('util');

var helper = require('./helper.js');



///--- Globals

var uuid = {
    v1: libuuid.create,
    v4: libuuid.create
};

var BUCKET_CFG = {
    index: {
        str: {
            type: 'string'
        },
        str_u: {
            type: 'string',
            unique: true
        },
        str_2: {
            type: 'string'
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
    pre: [function (req, cb) {
        var v = req.value;
        if (v.pre)
            v.pre = 'pre_overwrite';

        cb();
    }],
    post: [function (req, cb) {
        cb();
    }],
    options: {
        version: 1,
        trackModification: true,
        guaranteeOrder: true
    }
};

var c; // client
var server;
var b; // bucket

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
        b = 'moray_unit_test_' + uuid.v4().substr(0, 7);
        helper.createServer(function (s) {
            server = s;
            c = helper.createClient();
            c.on('connect', function () {
                c.createBucket(b, BUCKET_CFG, function (err) {
                    t.ifError(err);
                    t.end();
                });
            });
        });
    });

    tape.test(name + ' - main', function (t) {
        setup(t);
    });

    tape.test(name + ' - teardown', function (t) {
        c.delBucket(b, function (err) {
            t.ifError(err);
            c.once('close', function () {
                helper.cleanupServer(server);
                t.end();
            });
            c.close();
        });
    });
}


///--- Helpers

function assertObject(t, obj, k, v) {
    t.ok(obj);
    if (!obj)
        return (undefined);

    t.equal(obj.bucket, b);
    t.equal(obj.key, k);
    t.deepEqual(obj.value, v);
    t.ok(obj._id);
    t.ok(obj._etag);
    t.ok(obj._mtime);
    if (v.vnode) {
        t.ok(obj.value.vnode);
    }
    return (undefined);
}

///--- Tests

test('get object 404', function (t) {
    c.getObject(b, uuid.v4().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'ObjectNotFoundError');
        t.ok(err.message);
        t.end();
    });
});


test('del object 404', function (t) {
    c.delObject(b, uuid.v4().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'ObjectNotFoundError');
        t.ok(err.message);
        t.end();
    });
});


test('CRUD object', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi',
        vnode: 2
    };
    var v2 = {
        str: 'hello world',
        pre: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                return (cb());
            });
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                v2.pre = 'pre_overwrite';
                assertObject(t, obj, k, v2);
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('get object (cached)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                return (cb());
            });
        }, function getAgain(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                return (cb());
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('CRUD objects unique indexes', function (t) {
    var k = uuid.v4();
    var k2 = uuid.v4();
    var v = {
        str_u: 'hi'
    };
    var v2 = {
        str_u: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function putFail(_, cb) {
            c.putObject(b, k2, v2, function (err) {
                t.ok(err);
                t.equal(err.name, 'UniqueAttributeError');
                cb();
            });
        }, function delK1(_, cb) {
            c.delObject(b, k, cb);
        }, function putK2(_, cb) {
            c.putObject(b, k2, v2, cb);
        }, function delK2(_, cb) {
            c.delObject(b, k2, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('put object w/etag ok', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world'
    };
    var etag;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                etag = obj._etag;
                return (cb());
            });
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, {etag: etag}, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v2);
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('del object w/etag ok', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var etag;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                etag = obj._etag;
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, {etag: etag}, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('put object w/etag conflict', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function overwrite(_, cb) {
            c.putObject(b, k, {}, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err)
                    t.equal(err.name, 'EtagConflictError');
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('del object w/etag conflict', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function drop(_, cb) {
            c.delObject(b, k, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err) {
                    t.equal(err.name, 'EtagConflictError');
                    t.ok(err.context);
                    if (err.context) {
                        var ctx = err.context;
                        t.equal(ctx.bucket, b);
                        t.equal(ctx.key, k);
                        t.equal(ctx.expected, 'foo');
                        t.ok(ctx.actual);
                    }
                }
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('MANTA-980 - null etag support', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world'
    };
    var etag;
    var value;

    function get_cb(cb) {
        function _cb(err, obj) {
            if (err) {
                cb(err);
                return;
            }

            t.ok(obj);
            if (obj) {
                assertObject(t, obj, k, value);
                etag = obj._etag;
            }
            cb();
        }
        return (_cb);
    }

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            value = v;
            c.putObject(b, k, value, {etag: null}, cb);
        }, function get(_, cb) {
            c.getObject(b, k, get_cb(cb));
        }, function overwrite(_, cb) {
            value = v2;
            c.putObject(b, k, value, {etag: etag}, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, get_cb(cb));
        }, function putFail(_, cb) {
            c.putObject(b, k, v, {etag: null}, function (err) {
                t.ok(err);
                if (err) {
                    t.equal(err.name, 'EtagConflictError');
                    t.ok(err.context);
                    t.equal(err.context.bucket, b);
                    t.equal(err.context.key, k);
                    t.equal(err.context.expected, 'null');
                    t.equal(err.context.actual, etag);
                }
                cb();
            });
        }, function del(_, cb) {
            c.delObject(b, k, {etag: etag}, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('find (like marlin)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(str=hello)(!(str_2=usa)))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._count);
                t.equal(typeof (obj._count), 'number');
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find _mtime', function (t) {
    var k = uuid.v4();
    var now = Date.now();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            /* this is sensitive to clock skew between hosts */
            setTimeout(cb, 1000);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(_mtime>=' + now + ')';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find _key', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(_key=' + k + ')';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find MANTA-156', function (t) {
    var k = uuid.v4();
    var v = {
        num: 0,
        num_u: 1
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(num>=0)';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('non-indexed AND searches (MANTA-317)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        cow: 'moo'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(str=hello)(!(cow=woof)))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('_txn_snap on update', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var txn;

    vasync.pipeline({
        funcs: [ function create(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getOne(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    assertObject(t, obj, k, v);
                    t.ok(obj._txn_snap);
                    txn = obj._txn_snap;
                    cb();
                }
            });
        }, function update(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTwo(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    assertObject(t, obj, k, v);
                    t.ok(obj._txn_snap);
                    t.notEqual(txn, obj._txn_snap);
                    t.ok(obj._txn_snap > txn);
                    cb();
                }
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('find _txn_snap', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(_txn_snap>=1)(_id>=1))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                t.ok(obj._txn_snap);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});



test('trackModification (MANTA-269)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var id1;

    vasync.pipeline({
        funcs: [ function create(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getOne(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    assertObject(t, obj, k, v);
                    id1 = obj._id;
                    cb();
                }
            });
        }, function update(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTwo(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    assertObject(t, obj, k, v);
                    t.notEqual(id1, obj._id);
                    cb();
                }
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('batch put objects', function (t) {
    var requests = [
        {
            bucket: b,
            key: uuid.v4(),
            value: {
                foo: 'bar'
            }
        },
        {
            bucket: b,
            key: uuid.v4(),
            value: {
                bar: 'baz'
            }
        }
    ];

    c.batch(requests, function (err, meta) {
        t.ifError(err);
        t.ok(meta);
        if (meta) {
            t.ok(meta.etags);
            if (meta.etags) {
                t.ok(Array.isArray(meta.etags));
                t.equal(meta.etags.length, 2);
                meta.etags.forEach(function (e) {
                    t.equal(b, e.bucket);
                    t.ok(e.key);
                    t.ok(e.etag);
                });
            }
        }
        c.getObject(b, requests[0].key, function (er2, obj) {
            t.ifError(er2);
            t.ok(obj);
            if (obj)
                t.deepEqual(obj.value, requests[0].value);

            var r = requests[1];
            c.getObject(b, r.key, function (err3, obj2) {
                t.ifError(err3);
                t.ok(obj2);
                if (obj2)
                    t.deepEqual(obj2.value, r.value);
                t.end();
            });
        });
    });
});


test('update objects no keys', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        c.updateObjects(b, {}, '(num>=20)', function (err) {
            t.ok(err);
            t.equal(err.name, 'FieldUpdateError');
            t.end();
        });
    });
});


test('update objects ok', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        var fields = {str: 'bar'};
        c.updateObjects(b, fields, '(num>=20)', function (err, meta) {
            t.ifError(err);
            t.ok(meta);
            if (!meta) {
                t.end();
                return;
            }
            t.ok(meta.etag);

            c.getObject(b, requests[0].key, function (err2, obj) {
                t.ifError(err2);
                t.ok(obj);
                if (obj) {
                    t.equal(obj.value.str, 'bar');
                    t.equal(obj._etag, meta.etag);
                }

                t.end();
            });
        });
    });
});


test('update objects w/array (ufds - no effect)', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                str: ['foo']
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        var fields = {str: 'bar'};
        c.updateObjects(b, fields, '(str=foo)', function (err, meta) {
            t.ifError(err);
            t.ok(meta);
            if (!meta) {
                t.end();
                return;
            }
            t.ok(meta.etag);

            var k = requests[0].key;
            var o = {noCache: true};
            c.getObject(b, k, o, function (err2, obj) {
                t.ifError(err2);
                t.ok(obj);
                if (obj) {
                    t.ok(Array.isArray(obj.value.str));
                    t.notOk(obj.value.str_u);
                    t.equal(obj.value.str[0], 'foo');
                    t.equal(obj._etag, meta.etag);
                }

                t.end();
            });
        });
    });
});


test('batch put/update', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (init_err) {
        t.ifError(init_err);

        var ops = [
            {
                bucket: b,
                key: requests[0].key,
                value: {
                    num: 10,
                    str: 'baz'
                }
            },
            {
                bucket: b,
                operation: 'update',
                fields: {
                    str: 'bar'
                },
                filter: '(num_u>=5)'
            }
        ];
        c.batch(ops, function (err, meta) {
            t.ifError(err);
            t.ok(meta);
            t.ok(meta.etags);
            var req = c.findObjects(b, '(num_u>=0)');
            req.once('error', function (e) {
                t.ifError(e);
                t.end();
            });
            req.once('end', function () {
                t.end();
            });
            req.on('record', function (r) {
                t.equal(r.bucket, b);
                t.ok(r.key);
                var v = r.value;
                if (v.num_u >= 5) {
                    t.equal(v.str, 'bar');
                } else if (r.key === requests[0].key) {
                    t.equal(v.str, 'baz');
                } else {
                    t.equal(v.str, 'foo');
                }
            });
        });
    });
});


test('delete many objects ok', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        c.deleteMany(b, '(num>=20)', function (err) {
            t.ifError(err);
            t.end();
        });
    });
});

test('get tokens unsupported', function (t) {
    c.getTokens(function (err, res) {
        t.notOk(res);
        t.ok(err);
        t.end();
    });
});


test('MORAY-147 (sqli)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(str=hel\')(!(str_2=usa)))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                found = true;
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(!found);
        t.end();
    });
});



test('MORAY-148 (foo=bar=*)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello=world',
        str_2: 'world=hello'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(|(str=hello=*)(str_2=world=*))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                found = true;
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('MORAY-166: deleteMany with LIMIT', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello=world'
    };
    var N = 35;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            cb = once(cb);

            var done = 0;
            function _cb(err) {
                if (err) {
                    cb(err);
                } else if (++done === N) {
                    cb();
                }
            }

            for (var i = 0; i < N; i++)
                c.putObject(b, k + '' + i, v, _cb);

        }, function delMany(_, cb) {
            cb = once(cb);

            var _opts = {
                limit: Math.floor(N / 4)
            };

            (function drop() {
                function _cb(err, meta) {
                    if (err) {
                        cb(err);
                        return;
                    }

                    t.ok(meta);
                    if (!meta) {
                        cb(new Error('boom'));
                        return;
                    }
                    t.ok(meta.count <= _opts.limit);
                    if (meta.count > 0) {
                        drop();
                    } else {
                        cb();
                    }
                }

                c.deleteMany(b, '(str=*)', _opts, _cb);
            })();
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});



test('MORAY-166: update with LIMIT', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello=world'
    };
    var N = 35;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            cb = once(cb);

            var done = 0;
            function _cb(err) {
                if (err) {
                    cb(err);
                } else if (++done === N) {
                    cb();
                }
            }

            for (var i = 0; i < N; i++)
                c.putObject(b, k + '' + i, v, _cb);

        }, function updateMany(_, cb) {
            cb = once(cb);

            var _opts = {
                limit: Math.floor(N / 4)
            };

            function _cb(err, meta) {
                if (err) {
                    cb(err);
                    return;
                }

                t.ok(meta);
                if (!meta) {
                    cb(new Error('boom'));
                    return;
                }

                t.equal(meta.count, _opts.limit);
                cb();
            }

            c.updateObjects(b, {str: 'fo'}, '(str=*)', _opts, _cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MORAY-166: delete w/LIMIT in batch', function (t) {
    var k = uuid.v4();

    vasync.pipeline({
        funcs: [
            function putObjects(_, cb) {
                cb = once(cb);
                var barrier = vasync.barrier();
                var vals = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
                vals.forEach(function (i) {
                    barrier.start(i);
                    var _k = k + i;
                    var v = {
                        num: i
                    };

                    c.putObject(b, _k, v, function (err) {
                        if (err)
                            cb(err);

                        barrier.done(i);
                    });
                });

                barrier.on('drain', cb);
            },
            function deleteObjects(_, cb) {
                cb = once(cb);
                c.batch([
                    {
                        operation: 'deleteMany',
                        bucket: b,
                        filter: 'num=*',
                        options: {
                            limit: 5
                        }
                    }
                ], function (err, meta) {
                    if (err) {
                        cb(err);
                        return;
                    }
                    t.ok(meta);
                    t.equal(meta.etags[0].count, 5);
                    cb();
                });
            }
        ]
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MORAY-175: overwrite with \' in name', function (t) {
    var k = uuid.v4() + '\'foo';
    var v = {
        str: 'hi',
        vnode: 2
    };
    var v2 = {
        str: 'hello world',
        pre: 'hi'
    };

    vasync.pipeline({
        funcs: [ function create(_, cb) {
            c.putObject(b, k, v, cb);
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    v2.pre = 'pre_overwrite';
                    assertObject(t, obj, k, v2);
                    cb();
                }
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('reindex objects', function (t) {

    var field = 'unindexed';
    var COUNT = 1000;
    var PAGESIZE = 100;
    var records = [];
    for (var i = 0; i < COUNT; i++) {
        records.push(i);
    }

    vasync.pipeline({
        funcs: [
            function insertRecords(_, cb) {
                vasync.forEachPipeline({
                    func: function (id, callback) {
                        var k = uuid.v4();
                        var obj = {
                            str: 'test'
                        };
                        obj[field] = id;
                        c.putObject(b, k, obj, function (err, meta) {
                            callback(err);
                        });
                    },
                    inputs: records
                }, function (err) {
                    t.ifError(err);
                    t.ok(true, 'insert records');
                    cb(err);
                });
            },
            function updateBucket(_, cb) {
                var config = clone(BUCKET_CFG);
                config.index[field] =  {type: 'number'};
                config.options.version++;
                c.updateBucket(b, config, function (err) {
                    t.ifError(err);
                    t.ok(true, 'update bucket');
                    cb(err);
                });
            },
            function reindexObjects(_, cb) {
                var total = 0;
                function runReindex() {
                    c.reindexObjects(b, PAGESIZE, function (err, res) {
                        if (err) {
                            t.ifError(err);
                            cb(err);
                            return;
                        }
                        if (res.processed === 0) {
                            t.equal(COUNT, total);
                            cb();
                        } else {
                            total += res.processed;
                            process.nextTick(runReindex);
                        }
                    });
                }
                runReindex();
            },
            function queryNewIndex(_, cb) {
                var limit = COUNT / 2;
                var filter = util.format('(%s<=%d)', field, limit);

                var found = 0;
                var opts = {
                    noBucketCache: true
                };
                var res = c.findObjects(b, filter, opts);
                res.on('error', cb);
                res.on('record', function () {
                    found++;
                });
                res.on('end', function () {
                    // <= means limit+1
                    t.equal(limit+1, found);
                    cb();
                });
            }
        ]
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});
