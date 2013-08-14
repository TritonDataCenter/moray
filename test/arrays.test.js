// Copyright 2013 Joyent.  All rights reserved.

var once = require('once');
var uuid = require('node-uuid');
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


test('schema array, array value (string)', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        name: {
                                type: '[string]',
                                unique: false
                        }
                }
        };
        var data = {
                name: ['foo', 'bar', 'baz'],
                ignoreme: 'foo'
        };

        vasync.pipeline({
                funcs: [
                        function bucket(_, cb) {
                                c.putBucket(b, cfg, cb);
                        },
                        function object(_, cb) {
                                c.putObject(b, k, data, cb);
                        },
                        function find(_, cb) {
                                cb = once(cb);

                                var found = false;
                                var req = c.findObjects(b, '(name=foo)');
                                req.once('error', cb);
                                req.once('record', function (obj) {
                                        t.ok(obj);
                                        if (obj) {
                                                t.equal(obj.bucket, b);
                                                t.equal(obj.key, k);
                                                t.deepEqual(obj.value, data);
                                                t.ok(obj._id);
                                                t.ok(obj._etag);
                                                t.ok(obj._mtime);
                                                found = true;
                                        }
                                });
                                req.once('end', function () {
                                        t.ok(found);
                                        cb();
                                });
                        }
                ],
                arg: null
        }, function (err) {
                t.ifError(err);
                t.end();
        });
});
