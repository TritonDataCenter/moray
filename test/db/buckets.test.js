// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// You can set PG_URL to connect to a database, and LOG_LEVEL to turn on
// bunyan debug logs.
//

var uuid = require('node-uuid');

var db = require('../../lib/db');
var errors = require('../../lib/errors');

var helper = require('../helper');



///--- Globals

var test = helper.test;

var BM;
var BUCKET = process.env.BUCKET || 'a' + uuid().replace('-', '').substr(0, 7);
var KEY = '/foo/bar';
var PG;
var SCHEMA = {
    email: {
        type: 'string',
        unique: true
    },
    department: {
        type: 'number',
        unique: false
    },
    isManager: {
        type: 'boolean',
        unique: false
    }
};
var URL = process.env.DATABASE_URL || 'pg://unit:test@localhost/test';



///--- Tests

test('create postgres client', function (t) {
    PG = new db.Postgres({
        log: helper.log,
        url: URL
    });
    t.ok(PG);
    db.createBucketManager({
        log: helper.log,
        pgClient: PG
    }, function (err, bm) {
        t.ifError(err);
        BM = bm;
        t.done();
    });
});


test('list buckets (none)', function (t) {
    BM.list(function (err, buckets) {
        t.ifError(err);
        t.equal(buckets[BUCKET], undefined);
        t.done();
    });
});


test('create bucket invalid name', function (t) {
    t.throws(function () {
        BM.put(uuid(), {}, function (err, bucket) {
            t.fail('create ran');
        });
    }, InvalidBucketNameError);
    t.done();
});


test('create bucket ok no schema', function (t) {
    BM.put(BUCKET, {}, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('create bucket ok full schema', function (t) {
    BM.put(BUCKET, {schema: SCHEMA}, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('list buckets', function (t) {
    BM.list(function (err, buckets) {
        t.ifError(err);
        t.ok(buckets);
        t.ok(buckets[BUCKET]);
        t.deepEqual(buckets[BUCKET].schema, SCHEMA);
        t.done();
    });
});


test('list keys', function (t) {
    BM.keys(BUCKET, function (err, obj) {
        t.ifError(err);
        t.ok(obj);
        t.equal(obj.total, 0);
        t.ok(obj.keys);
        t.equal(Object.keys(obj.keys).length, 0);
        t.done();
    });
});


test('cleanup bucket', function (t) {
    BM.del(BUCKET, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('destroy postgres client', function (t) {
    PG.shutdown(function () {
        t.done();
    });
});
