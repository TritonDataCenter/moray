// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// You can set PG_URL to connect to a database, and LOG_LEVEL to turn on
// bunyan debug logs.
//

var uuid = require('node-uuid');

var db = require('../lib/db');
var errors = require('../lib/errors');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var test = helper.test;

var BUCKET = process.env.BUCKET || 'a' + uuid().replace('-', '').substr(0, 7);
var KEY = uuid();
var OBJECT = {
    email: 'mark.cavage@joyent.com',
    department: 123
};
var OM;
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

test('create object manager', function (t) {
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
        db.createObjectManager({
            bucketManager: bm,
            log: helper.log,
            pgClient: PG
        }, function (err2, om) {
            t.ifError(err2);
            OM = om;
            t.done();
        });
    });
});


test('setup bucket', function (t) {
    OM.bucketManager.put(BUCKET, {schema: SCHEMA}, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('put object ok', function (t) {
    OM.put(BUCKET, KEY, OBJECT, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('get object ok', function (t) {
    OM.get(BUCKET, KEY, function (err, object) {
        t.ifError(err);
        t.ok(object);
        t.deepEqual(object.value, OBJECT);
        t.equal(object.bucket, BUCKET);
        t.equal(object.key, KEY);
        t.ok(object.etag);
        t.ok(object.mtime);
        t.done();
    });
});


test('del object ok', function (t) {
    OM.del(BUCKET, KEY, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('get object tombstone ok', function (t) {
    OM.getTombstone(BUCKET, KEY, function (err, object) {
        t.ifError(err);
        t.ok(object);
        t.deepEqual(object.value, OBJECT);
        t.equal(object.bucket, BUCKET);
        t.equal(object.key, KEY);
        t.ok(object.etag);
        t.ok(object.dtime);
        t.done();
    });
});


test('restore object ok', function (t) {
    OM.restore(BUCKET, KEY, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('find objects ok', function (t) {
    var query =
        '(&(!(department=234))(|(email=*@joyent.com)(email=mark*))';
    OM.find(BUCKET, query, function (err, results) {
        t.ifError(err);
        //console.log(results)
        t.done();
    });
});


test('cleanup bucket', function (t) {
    OM.bucketManager.del(BUCKET, function (err) {
        t.ifError(err);
        t.done();
    });
});


test('shutdown', function (t) {
    PG.shutdown(function () {
        t.done();
    });
});
