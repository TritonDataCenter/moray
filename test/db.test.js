// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// You can set PG_URL to connect to a database, and LOG_LEVEL to turn off
// bunyan debug logs.
//

var assert = require('assert');
var util = require('util');

var Logger = require('bunyan');
var uuid = require('node-uuid');

var DB = require('../lib').DB;

var test = require('./helper').test;



///--- Globals

var BUCKET = process.env.BUCKET || 'a' + uuid().replace('-', '').substr(0, 7);
var CLIENT;
var DB_URL = process.env.DATABASE_URL || 'pg://unit:test@localhost/test';
var KEY = '/foo/bar';
var LOG = new Logger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: 'db.test.js',
    stream: process.stderr,
    src: true,
    serializers: Logger.stdSerializers
});



///--- Tests

test('missing options', function (t) {
    t.throws(function () {
        t.ok(new DB());
    }, TypeError);
    t.done();
});


test('missing logger', function (t) {
    t.throws(function () {
        t.ok(new DB({}));
    }, TypeError);
    t.done();
});


test('missing url', function (t) {
    t.throws(function () {
        t.ok(new DB({log: LOG}));
    }, TypeError);
    t.done();
});


test('create ok', function (t) {
    CLIENT = new DB({
        log: LOG,
        url: DB_URL
    });
    t.ok(CLIENT);
    CLIENT.on('connect', function () {
        t.done();
    });
});

test('create bucket with no indexes', function (t) {
    CLIENT.createBucket(BUCKET, function (err) {
        t.ifError(err);
        t.done();
    });
});

test('delete bucket', function (t) {
    CLIENT.deleteBucket(BUCKET, function (err) {
        t.ifError(err);
        t.done();
    });
});

test('create bucket with indexes', function (t) {
    var indexes = {
        foo: {
            type: 'string'
        },
        bar: {
            type: 'string',
            unique: true
        },
        num: {
            type: 'number'
        },
        bool: {
            type: 'boolean'
        }
    };
    CLIENT.createBucket(BUCKET, indexes, function (err) {
        t.ifError(err);
        t.done();
    });
});

test('list buckets', function (t) {
    CLIENT.buckets(function (err, buckets) {
        t.ifError(err);
        t.ok(buckets);
        t.equal(typeof (buckets), 'object');
        t.ok(buckets[BUCKET]);
        t.ok(buckets[BUCKET].foo);
        t.ok(buckets[BUCKET].bar);
        t.equal(buckets[BUCKET].foo.type, 'string');
        t.equal(buckets[BUCKET].foo.unique, false);
        t.equal(buckets[BUCKET].bar.type, 'string');
        t.equal(buckets[BUCKET].bar.unique, true);
        t.equal(buckets[BUCKET].num.type, 'number');
        t.equal(buckets[BUCKET].num.unique, false);
        t.equal(buckets[BUCKET].bool.type, 'boolean');
        t.equal(buckets[BUCKET].bool.unique, false);
        t.done();
    });
});


test('put k/v', function (t) {
    CLIENT.put(BUCKET, KEY, {foo: 'foo', bar: 'bar'}, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('get k/v (original)', function (t) {
    CLIENT.get(BUCKET, KEY, function (err, obj) {
        t.ifError(err);
        t.ok(obj);
        console.log(obj);
        t.equal(obj.bucket, BUCKET);
        t.equal(obj.key, KEY);
        t.equal(obj.data.foo, 'foo');
        t.equal(obj.data.bar, 'bar');
        t.ok(obj.etag);
        t.ok(obj.mtime);
        t.done();
    });
});


test('get key not found', function (t) {
    CLIENT.get(BUCKET, uuid(), function (err, obj) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.done();
    });
});


test('overwrite k/v', function (t) {
    CLIENT.put(BUCKET, KEY, {foo: 'cow', bar: 'horse'}, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('get k/v (overwrite)', function (t) {
    CLIENT.get(BUCKET, KEY, function (err, obj) {
        t.ifError(err);
        t.ok(obj);
        t.equal(obj.bucket, BUCKET);
        t.equal(obj.key, KEY);
        t.equal(obj.data.foo, 'cow');
        t.equal(obj.data.bar, 'horse');
        t.ok(obj.etag);
        t.ok(obj.mtime);
        t.done();
    });
});


test('restore from tombstone', function (t) {
    CLIENT.restore(BUCKET, KEY, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('get k/v (original - restored)', function (t) {
    CLIENT.get(BUCKET, KEY, function (err, obj) {
        t.ifError(err);
        t.ok(obj);
        t.equal(obj.bucket, BUCKET);
        t.equal(obj.key, KEY);
        t.equal(obj.data.foo, 'foo');
        t.equal(obj.data.bar, 'bar');
        t.ok(obj.etag);
        t.ok(obj.mtime);
        t.done();
    });
});


test('find ok', function (t) {
    CLIENT.find(BUCKET, 'foo', 'f', function (err, query) {
        assert.ifError(err);
        t.ok(query);
        query.on('error', function (error) {
            t.fail('find error: ' + error.stack);
        });
        query.on('entry', function (obj) {
            t.ok(obj);
            t.equal(obj.bucket, BUCKET);
            t.equal(obj.key, KEY);
            t.equal(obj.data.foo, 'foo');
            t.equal(obj.data.bar, 'bar');
            t.ok(obj.etag);
            t.ok(obj.mtime);
        });
        query.on('end', function () {
            t.done();
        });
    });
});


test('list ok', function (t) {
    CLIENT.list(BUCKET, {prefix: '/f', limit: 1}, function (err, keys) {
        assert.ifError(err);
        t.ok(keys);
        t.ok(Array.isArray(keys.keys));
        t.equal(keys.total, keys.keys.length);
        t.equal(keys.keys[0].key, '/foo/bar');
        t.ok(keys.keys[0].etag);
        t.ok(keys.keys[0].mtime);
        t.done();
    });
});


test('delete k/v', function (t) {
    CLIENT.del(BUCKET, KEY, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('delete key not found', function (t) {
    CLIENT.del(BUCKET, uuid(), function (err) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.done();
    });
});


test('restore not found', function (t) {
    CLIENT.restore(BUCKET, uuid(), function (err) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.done();
    });
});


test('get not found (after delete)', function (t) {
    CLIENT.get(BUCKET, KEY, function (err, obj) {
        t.ok(err);
        t.equal(err.name, 'ResourceGoneError');
        t.done();
    });
});


test('cleanup', function (t) {
    CLIENT.deleteBucket(BUCKET, function (err) {
        assert.ifError(err);
        CLIENT.end();
        t.done();
    });
});
