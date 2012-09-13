// Copyright 2012 Joyent.  All rights reserved.

var uuid = require('node-uuid');

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


test('MANTA-117 single quotes not being escaped', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid();
        var cfg = {
                index: {
                        name: {
                                type: 'string',
                                unique: true
                        }
                }
        };
        var data = {
                name: uuid(),
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
