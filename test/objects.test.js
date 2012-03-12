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
        bucket: BUCKET,
        index: {
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
    CLIENT.post('/', opts, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj);
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
