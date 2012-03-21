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
var ETAG;
var SERVER;


///--- Helpers

function key(k) {
    return '/' + BUCKET + '/' + encodeURIComponent(k);
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


test('create bucket', function (t) {
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
            name: {
                type: 'string'
            }
        }
    };
    CLIENT.put('/' + BUCKET, opts, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.done();
    });
});


test('put object bad bucket', function (t) {
    CLIENT.put('/a' + uuid().substr(0, 7) + '/foo', {}, function (err) {
        t.ok(err);
        t.equal(err.code, 'ResourceNotFound');
        t.ok(err.message);
        t.done();
    });
});


test('put object ok', function (t) {
    var data = {
        id: 1,
        email: 'mark.cavage@joyent.com',
        name: 'mark'
    };
    CLIENT.put(key('mark'), data, function (err, req, res) {
        t.ifError(err);
        t.ok(res.headers['etag']);
        ETAG = res.headers['etag'];
        t.equal(res.statusCode, 204);
        t.done();
    });
});


test('get object ok', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-Match': ETAG
        }
    };
    CLIENT.get(opts, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.equal(obj.id, 1);
        t.done();
    });
});


test('get object 304', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-None-Match': ETAG
        }
    };
    CLIENT.get(opts, function (err, req, res, obj) {
        t.ifError(err);
        t.equal(res.statusCode, 304);
        t.done();
    });
});


test('get object 412', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-Match': uuid()
        }
    };
    CLIENT.get(opts, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'PreconditionFailed');
        t.ok(err.message);
        t.done();
    });
});


test('put object 412', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-Match': uuid()
        }
    };
    CLIENT.put(opts, {}, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.code, 'PreconditionFailed');
        t.ok(err.message);
        t.done();
    });
});


test('put object conditionally ok', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-Match': ETAG
        }
    };
    var body = {
        foo: 'bar',
        email: 'mark.cavage@joyent.com'
    };
    CLIENT.put(opts, body, function (err, req, res, obj) {
        t.ifError(err);
        ETAG = res.headers['etag'];
        t.done();
    });
});


test('put object unique attribute taken', function (t) {
    var data = {
        id: 2,
        email: 'mark.cavage@joyent.com',
        name: 'mark'
    };
    CLIENT.put(key('markus'), data, function (err, req, res) {
        t.ok(err);
        t.equal(err.code, 'InvalidArgument');
        t.equal(err.message,
                'Key (email)=(mark.cavage@joyent.com) already exists.');
        t.done();
    });
});


test('put object ok duplicate non-unique index', function (t) {
    var data = {
        id: 2,
        email: 'mcavage@gmail.com',
        name: 'mark'
    };
    CLIENT.put(key('mcavage'), data, function (err, req, res) {
        t.ifError(err);
        t.ok(res.headers['etag']);
        t.equal(res.statusCode, 204);
        t.done();
    });
});


test('get object ok', function (t) {
    CLIENT.get(key('mark'), function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
        t.equal(obj.id, 1);
        t.equal(obj.email, 'mark.cavage@joyent.com');
        t.equal(obj.name, 'mark');
        t.done();
    });
});


test('delete ok', function (t) {
    CLIENT.del(key('mcavage'), function (err) {
        t.ifError(err);
        t.done();
    });
});


test('delete object 412', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-Match': uuid()
        }
    };
    CLIENT.del(opts, function (err) {
        t.ok(err);
        t.equal(err.code, 'PreconditionFailed');
        t.ok(err.message);
        t.done();
    });
});


test('delete object conditionally ok', function (t) {
    var opts = {
        path: key('mark'),
        headers: {
            'If-Match': ETAG
        }
    };
    CLIENT.del(opts, function (err) {
        t.ifError(err);
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
