// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var BucketManager = require('./buckets').BucketManager;
var ObjectManager = require('./objects').ObjectManager;
var Postgres = require('./pg_client');
var SQL = require('./sql');

require('../args');



///--- API

function createPgClient(options) {
    assertObject('options', options);
    assertObject('options.log', options.log);
    assertString('options.url', options.url);

    return new Postgres(options);
}


function createBucketManager(options, callback) {
    assertObject('options', options);
    assertObject('options.log', options.log);
    if (options.pgClient)
        assertObject('options.pgClient', options.pgClient);
    if (options.url)
        assertString('options.url', options.url);
    if (!options.pgClient === !options.url)
        throw new Error('Only (options.pgClient ^ options.url) allowed');
    assertFunction('callback', callback);

    var pgClient = options.pgClient || createPgClient(options);

    var bm = new BucketManager({
        log: options.log,
        pgClient: pgClient
    });

    bm.once('error', function (err) {
        return callback(err);
    });
    bm.on('ready', function () {
        bm.removeAllListeners('error');
        return callback(null, bm);
    });

    return bm;
}


function createObjectManager(options, callback) {
    assertObject('options', options);
    assertObject('options.bucketManager', options.bucketManager);
    assertObject('options.log', options.log);
    assertObject('options.pgClient', options.pgClient);
    assertFunction('callback', callback);

    var pgClient = options.pgClient || createPgClient(options);

    if (!options.bucketManager) {
        return createBucketManager(options, function (err, bm) {
            if (err)
                return callback(err);

            return callback(null, new ObjectManager({
                bucketManager: bm,
                log: options.log,
                pgClient: pgClient
            }));
        });
    }

    return callback(null, new ObjectManager({
        bucketManager: options.bucketManager,
        log: options.log,
        pgClient: pgClient
    }));
}



///--- Exports

module.exports = {

    BucketManager: BucketManager,
    ObjectManager: ObjectManager,
    Postgres: Postgres,
    SQL: SQL,
    createBucketManager: createBucketManager,
    createObjectManager: createObjectManager,
    createPgClient: createPgClient

};
