// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');

var pg = require('./pg');



///--- Globals

var sprintf = util.format;



///--- Handlers

function commitTransaction(req, res, next) {
        req.pg.commit(req.client, function (err) {
                req.client = null;
                next(err);
        });
}


function loadBucket(req, res, next) {
        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('SELECT * FROM buckets_config WHERE name=\'%s\'',
                             req.params.bucket)
        };

        log.debug({
                bucket: req.params.bucket
        }, 'loadBucket: entered');
        req.pg.query(opts, function (err, rows) {
                if (err) {
                        log.debug({
                                bucket: req.params.bucket,
                                err: err
                        }, 'loadBucket: failed');
                        next(err);
                } else {
                        if (rows.length > 0) {
                                assert.equal(rows.length, 1);
                                var b = rows[0];
                                var fn;
                                b.index = JSON.parse(b.index);
                                b.post = JSON.parse(b.post);
                                b.pre = JSON.parse(b.pre);

                                req.bucket = b;
                                req.bucket.post = b.post.map(function (p) {
                                        return (eval('fn = ' + p));
                                });
                                req.bucket.pre = b.pre.map(function (p) {
                                        return (eval('fn = ' + p));
                                });
                                // Make JSLint shutup
                                if (fn)
                                        fn = null;
                                // End Make JSLint shutup
                        }

                        log.debug({
                                bucket: req.params.bucket,
                                previous: req.bucket
                        }, 'loadBucket: done');
                        next();
                }
        });
}

function pause(req, res, next) {
        function _buffer(chunk) {
                req.__buffered.push(chunk);
        }

        req.__buffered = [];
        req.on('data', _buffer);
        req.pause();

        req._resume = req.resume;
        req.resume = function myResume() {
                req.removeListener('data', _buffer);
                req.__buffered.forEach(req.emit.bind(req, 'data'));
                req.__buffered = null;
                req._resume();

                // check if the request already ended
                if (!req.readable)
                        req.emit('end');
        };

        next();
}



function resume(req, res, next) {
        process.nextTick(function () {
                req.resume();
        });
        next();
}


function rollbackTransaction(req, res, next) {
        if (typeof (next) !== 'function') {
                next = function() {};
        }

        if (req.pg && req.client) {
                console.log(req.client)
                req.pg.rollback(req.client, next);
        } else {
                next();
        }
}


function startTransaction(req, res, next) {
        var log = req.log;

        log.debug('starting transaction');
        req.pg.start(function (err, client) {
                if (err) {
                        next(err);
                } else {
                        req.client = client;
                        next();
                }
        });
}



///--- Exports

module.exports = {
        commitTransaction: commitTransaction,
        loadBucket: loadBucket,
        pause: pause,
        resume: resume,
        rollbackTransaction: rollbackTransaction,
        startTransaction: startTransaction
};