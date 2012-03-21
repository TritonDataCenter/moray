// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// You can set PG_URL to connect to a database, and LOG_LEVEL to turn on
// bunyan debug logs.
//

var assert = require('assert');
var util = require('util');

var Logger = require('bunyan');
var uuid = require('node-uuid');

var helper = require('./helper');



///--- Globals

var test = helper.test;

var BUCKET = process.env.BUCKET || 'a' + uuid().replace('-', '').substr(0, 7);
var CLIENT;
var SERVER;



///--- Common checks

function checkBucketSchema(t, obj) {
    t.equal(obj.id.type, 'number');
    t.equal(obj.id.unique, true);
    t.equal(obj.email.type, 'string');
    t.equal(obj.email.unique, true);
    t.equal(obj.age.type, 'number');
    t.ok(!obj.age.unique);
    t.equal(obj.ismanager.type, 'boolean');
    t.ok(!obj.ismanager.unique);
}



///--- Tests

test('start server', function (t) {
    helper.createServer(function (err, server) {
        t.ifError(err);
        t.ok(server);
        SERVER = server;
        SERVER.start(function () {
            CLIENT = helper.createClient();
            t.ok(CLIENT);
            t.done();
        });
    });
});


test('create invalid bucket', function (t) {
    CLIENT.put('/' + uuid(), {}, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'InvalidArgument');
        t.ok(err.message);
        t.done();
    });
});


test('create reserved bucket', function (t) {
    CLIENT.put('/search', {}, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'InvalidArgument');
        t.ok('search is a reserved name');
        t.done();
    });
});


test('create index bad type', function (t) {
    var opts = {
        schema: {
            index: 9
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'InvalidArgument');
        t.ok(err.message);
        t.done();
    });
});


test('create index bad type (array)', function (t) {
    var opts = {
        schema: {
            index: [9]
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'InvalidArgument');
        t.ok(err.message);
        t.done();
    });
});


test('create index object ok index type bad', function (t) {
    var opts = {
        schema: {
            'foo': {
                type: 'foo'
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'InvalidArgument');
        t.ok(err.message);
        t.done();
    });
});


test('create bucket ok string index', function (t) {
    var opts = {
        schema: {
            foo: {
                type: 'string'
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res) {
        t.ifError(err);
        CLIENT.del('/' + BUCKET, function (err2) {
            t.ifError(err2);
            t.done();
        });
    });
});


test('create bucket ok object string, non-unique', function (t) {
    var opts = {
        schema: {
            foo: {
                type: 'string',
                unique: false
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res) {
        t.ifError(err);
        CLIENT.del('/' + BUCKET, function (err2) {
            t.ifError(err2);
            t.done();
        });
    });
});


test('create bucket ok object string, unique', function (t) {
    var opts = {
        schema: {
            foo: {
                type: 'string',
                unique: true
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res) {
        t.ifError(err);
        CLIENT.del('/' + BUCKET, function (err2) {
            t.ifError(err2);
            t.done();
        });
    });
});


test('create bucket ok object number, unique', function (t) {
    var opts = {
        schema: {
            foo: {
                type: 'number',
                unique: true
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res) {
        t.ifError(err);
        CLIENT.del('/' + BUCKET, function (err2) {
            t.ifError(err2);
            t.done();
        });
    });
});


test('create bucket ok indexes of all types', function (t) {
    var opts = {
        schema: {
            id: {
                type: 'number',
                unique: true
            },
            email: {
                type: 'string',
                unique: true
            },
            age: {
                type: 'number'
            },
            ismanager: {
                type: 'boolean'
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res) {
        t.ifError(err);
        t.done();
    });
});


test('list buckets ok', function (t) {
    CLIENT.get('/', function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.ok(obj[BUCKET]);
        t.ok(obj[BUCKET].schema);
        checkBucketSchema(t, obj[BUCKET].schema);
        t.done();
    });
});


test('get bucket 404', function (t) {
    CLIENT.get('/' + uuid(), function (err, req, res) {
        t.ok(err);
        t.equal(err.code, 'ResourceNotFound');
        t.ok(err.message);
        t.done();
    });
});

test('get bucket (schema) ok', function (t) {
    CLIENT.get('/' + BUCKET, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.ok(obj.schema);
        checkBucketSchema(t, obj.schema);
        t.done();
    });
});


test('put key #1', function (t) {
    var key = '/' + BUCKET + '/foo';
    CLIENT.put(key, {id: 1}, function (err, req, res, obj) {
        t.ifError(err);
        t.done();
    });
});


test('put key #2', function (t) {
    var key = '/' + BUCKET + '/bar';
    CLIENT.put(key, {id: 2}, function (err, req, res, obj) {
        t.ifError(err);
        t.done();
    });
});


test('list bucket keys', function (t) {
    CLIENT.get('/' + BUCKET, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.ok(obj.keys);
        t.equal(Object.keys(obj.keys).length, 2);
        t.done();
    });
});


test('page bucket keys', function (t) {
    CLIENT.get('/' + BUCKET + '?limit=1', function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.ok(obj.keys);
        t.equal(Object.keys(obj.keys).length, 1);
        t.ok(obj.keys.bar);
        t.ok(obj.keys.bar.etag);
        t.ok(obj.keys.bar.mtime);
        t.ok(res.headers['link']);
        var next = res.headers['link'].split(';')[0].substr(1);
        next = next.substr(0, next.length - 1);
        CLIENT.get(next, function (err2, req2, res2, obj2) {
            t.ifError(err2);
            t.ok(obj2.keys);
            t.equal(Object.keys(obj2.keys).length, 1);
            t.ok(obj2.keys.foo);
            t.ok(obj2.keys.foo.etag);
            t.ok(obj2.keys.foo.mtime);
            t.equal(res2.headers['link'], undefined);
            t.done();
        });
    });
});


test('list keys by prefix', function (t) {
    CLIENT.get('/' + BUCKET + '?prefix=f', function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.ok(obj.keys);
        t.equal(Object.keys(obj.keys).length, 1);
        t.ok(obj.keys.foo);
        t.ok(obj.keys.foo.etag);
        t.ok(obj.keys.foo.mtime);
        t.done();
    });
});


test('delete bucket', function (t) {
    CLIENT.del('/' + BUCKET, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('stop server', function (t) {
    SERVER.stop(function () {
        t.done();
    });
});
