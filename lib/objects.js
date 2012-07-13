// Copyright 2012 Joyent, Inc.  All rights reserved.

var crypto = require('crypto');
var util = require('util');

var assert = require('assert-plus');
var restify = require('restify');

var common = require('./common');
require('./errors');



///--- Globals

var sprintf = util.format;



///--- Helpers

function indexObject(schema, object) {
        assert.object(schema, 'schema');
        assert.object(object, 'object');

        var ndx = {};

        if (Object.keys(schema).length === 0)
                return (ndx);

        function index(k, v, t) {
                if (v !== undefined && typeof (v) !== t) {
                        if (typeof (v) === 'object') {
                                if (Array.isArray(v)) {
                                        v.forEach(function (v2) {
                                                index(k, v2, t);
                                        });
                                        return (undefined);
                                } else if (v === null) {
                                        return (false);
                                } else {
                                        throw new InvalidIndexTypeError(k, t);
                                }
                        }
                }

                switch (typeof (v)) {
                case 'boolean':
                case 'string':
                case 'number':
                        ndx[k] = v + '';
                        break;
                default:
                        break;
                }

                return (true);
        }

        Object.keys(schema).forEach(function (k) {
                index(k, object[k], schema[k].type);
        });

        return (ndx);
}



///--- Routes

//-- PUT Handlers

function selectForUpdate(req, res, next) {
        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('SELECT * FROM %s_entry WHERE _key=\'%s\' ' +
                             'FOR UPDATE', req.params.bucket, req.params.key)
        };

        log.debug({
                bucket: req.params.bucket,
                key: req.params.key
        }, 'selectForUpdate: entered');
        req.pg.query(opts, function (err, rows) {
                if (err) {
                        log.debug(err, 'selectForUpdate: failed');
                        next(err);
                } else {
                        if (rows.length > 0) {
                                req.previous = rows[0];
                                res.etag = rows[0].etag;
                        }

                        log.debug({
                                previous: req.previous || null
                        }, 'selectForUpdate: done');
                        next();
                }
        });
}


function index(req, res, next) {
        try {
                req.value = JSON.parse(req.body);
                req.index = indexObject(req.bucket.index, req.value);
        } catch (e) {
                return (next(e));
        }

        return (next());
}


function createEtagHash(req, res, next) {
        var hash = crypto.createHash('md5');
        hash.update(req.body);
        req.etag = hash.digest('hex');
        next();
}


function runPreChain(req, res, next) {
        var cookie = {
                bucket: req.params.bucket,
                key: req.params.key,
                log: req.log,
                pg: req.client,
                schema: req.bucket.index,
                value: req.value
        };
        var log = req.log;

        req.log.debug('runPreChain: entered');

        vasync.pipeline({
                funcs: req.bucket.pre,
                arg: cookie
        }, function (err, results) {
                if (err) {
                        next(err);
                } else {
                        var r = results.successes.pop();
                        if (r) {
                                req.params.bucket = r.bucket;
                                req.params.key = r.key;
                                req.params.value = r.value;
                        }
                        next();
                }
        });
}


function insert(req, res, next) {
        if (req.previous)
                return (next());

        req.log.debug({
                bucket: req.params.bucket,
                etag: req.etag,
                key: req.params.key,
                index: req.index,
                value: req.value
        }, 'insert: entered');

        var i = 0;
        var keyStr = '';
        var log = req.log;
        var opts;
        var valStr = '';
        var values = [req.params.key, JSON.stringify(req.value), req.etag];

        Object.keys(req.index).forEach(function (k) {
                values.push(req.index[k]);
                keyStr += ', ' + k;
                valStr += ', $' + values.length;
        });

        opts = {
                client: req.client,
                sql: sprintf('INSERT INTO %s_entry (_key, _value, _etag%s) ' +
                             'VALUES ($1, $2, $3%s) RETURNING _id',
                             req.params.bucket, keyStr, valStr),
                values: values
        };

        req.pg.query(opts, function (err) {
                log.debug(err, 'insert: %s', (err ? 'failed' : 'done'));
                next(err);
        });
        return (undefined);
}


function update(req, res, next) {
        if (!req.previous)
                return (next());

        req.log.debug({
                bucket: req.params.bucket,
                etag: req.etag,
                key: req.params.key,
                index: req.index,
                value: req.value
        }, 'update: entered');

        var badNews = false;
        var extraStr = '';
        Object.keys(req.bucket.index).forEach(function (k) {
                extraStr += ', ' + k + '='
                if (req.index[k]) {
                        switch (req.bucket.index[k].type) {
                        case 'boolean':
                                extraStr +=
                                req.index[k].toString().toUpperCase();
                                break;
                        case 'number':
                                extraStr += req.index[k];
                                break;
                        case 'string':
                                extraStr += '\'' + req.index[k] + '\'';
                                break;
                        default:
                                badNews = true;
                                break;
                        }
                } else {
                        extraStr += 'NULL';
                }
        });
        if (badNews) { // Deserves ALL CAPS!
                req.log.fatal('BAD SCHEMA IN DATABASE: %j', req.bucket.index);
                res.send(500);
                return (next());
        }

        var log = req.log;
        var opts = {
                client: req.client,
                sql: sprintf('UPDATE %s_entry SET ' +
                             '_value=\'%s\', _etag=\'%s\'%s' +
                             ' WHERE _key=\'%s\'',
                             req.params.bucket,
                             req.body,
                             req.etag,
                             extraStr,
                             req.params.key)
        };
        req.pg.query(opts, function (err) {
                log.debug(err, 'update: %s', (err ? 'failed' : 'done'));
                next(err);
        });
        return (undefined);
}


function putDone(req, res, next) {
        res.send(204);
        next();
}


//-- GET Handlers




///--- Exports

module.exports = {

        put: function put() {
                var chain = [
                        common.resume,
                        restify.bodyParser(),
                        common.startTransaction,
                        common.loadBucket,
                        selectForUpdate,
                        restify.conditionalRequest(),
                        createEtagHash,
                        index,
                        insert,
                        update,
                        common.commitTransaction,
                        putDone
                ];
                return (chain);
        }
};





// ///--- Helpers

// function load(req, res, next) {
//     var bucket = req.params.bucket;
//     var db = req.objectManager;
//     var key = req.params.key;
//     var log = req.log;
//     var getter = /.*\/tombstone$/.test(req.path) ? 'getTombstone' : 'get';

//     log.debug({
//         bucket: bucket,
//         key: key,
//         getter: getter
//     }, 'load entered');
//     return db[getter](bucket, key, function (err, obj) {
//         if (err) {
//             if (err.name === 'ResourceNotFoundError' && req.method === 'PUT')
//                 return next();

//             return next(err);
//         }

//         res.etag = obj.etag;
//         res.header('Last-Modified', obj.mtime);
//         res.header('X-Change-Number', obj.id);

//         req.bucket = bucket;
//         req.key = key;
//         req.object = obj.value;
//         log.debug({
//             bucket: req.bucket,
//             key: req.key
//         }, 'load -> %j', req.object);
//         return next();
//     });
// }



///--- Routes

// function put(req, res, next) {
//     var body = (typeof (req.body) === 'object' ? (req.body || {}) : {});
//     var bucket = req.params.bucket;
//     var db = req.objectManager;
//     var opts = { meta: {} };
//     var key = req.params.key;
//     var log = req.log;

//     log.debug({
//         bucket: bucket,
//         key: key,
//         body: body
//     }, 'PutObject entered');

//     if (req.header('if-match')) {
//         opts.match = true;
//         opts.value = req.header('if-match');
//     } else if (req.header('if-none-match')) {
//         opts.match = false;
//         opts.value = req.header('if-none-match');
//     }

//     Object.keys(req.headers).forEach(function (k) {
//         /* JSSTYLED */
//         if (/^x-.*/.test(k)) {
//             opts.meta[k] = req.headers[k];
//         }
//     });

//     return db.put(bucket, key, body, opts, function (err, etag2) {
//         if (err)
//             return next(err);

//         log.debug({
//             bucket: bucket,
//             key: key,
//             body: body
//         }, 'PutObject done');

//         res.writeHead(204, {
//             Connection: res.keepAlive ? 'Keep-Alive' : 'close',
//             'Content-Length': 0,
//             Etag: etag2,
//             'X-Request-Id': req.id,
//             'X-Response-Time': new Date().getTime() - req.time.getTime()
//         });
//         res.end();
//         return next();
//     });
// }


// function get(req, res, next) {
//     assert.ok(req.object);

//     req.log.debug({
//         bucket: req.params.bucket,
//         key: req.params.key
//     }, 'GetObject  -> %j', req.object);
//     res.send(req.object);
//     return next();
// }


// function del(req, res, next) {
//     var bucket = req.params.bucket;
//     var db = req.objectManager;
//     var key = req.params.key;
//     var log = req.log;
//     var opts = { meta: {} };

//     Object.keys(req.headers).forEach(function (k) {
//         /* JSSTYLED */
//         if (/^x-.*/.test(k)) {
//             opts.meta[k] = req.headers[k];
//         }
//     });

//     log.debug({bucket: bucket, key: key, opts: opts}, 'DeleteObject entered');
//     return db.del(bucket, key, opts, function (err) {
//         if (err)
//             return next(err);

//         log.debug({bucket: bucket, key: key}, 'DeleteObject done');
//         res.send(204);
//         return next();
//     });
// }


// function restore(req, res, next) {
//     var bucket = req.params.bucket;
//     var db = req.objectManager;
//     var key = req.params.key;
//     var log = req.log;

//     log.debug({bucket: bucket, key: key}, 'RestoreObject entered');
//     return db.restore(bucket, key, function (err, obj) {
//         if (err)
//             return next(err);

//         res.etag = obj.etag;
//         res.header('Last-Modified', obj.mtime);

//         log.debug({bucket: bucket, key: key}, 'RestoreObject -> %j', obj.data);
//         res.send(obj.data);
//         return next();
//     });
// }


// ///--- Exports

// module.exports = {
//     mount: function mount(server) {
//         args.assertArgument(server, 'object', server);

//         var bodyParser = restify.bodyParser({
//             mapParams: false
//         });
//         var pre = restify.conditionalRequest();

//         // The DB layer will kick a 412 if necessary on PUT, since the
//         // etag is checked as part of the TXN
//         server.put({path: '/:bucket/:key', name: 'PutObject'},
//                    bodyParser, put);

//         server.get({path: '/:bucket/:key', name: 'GetObject'},
//                    load, pre, get);
//         server.head({path: '/:bucket/:key', name: 'HeadObject'},
//                     load, pre, get);

//         server.del({path: '/:bucket/:key', name: 'DeleteObject'},
//                    load, pre, del);

//         server.get({
//             path: '/:bucket/:key/tombstone',
//             name: 'GetObjectTombstone'
//         }, load, pre, get);
//         server.head({
//             path: '/:bucket/:key/tombstone',
//             name: 'HeadObjectTombstone'
//         }, load, pre, get);

//         server.post({
//             path: '/:bucket/:key/tombstone',
//             name: 'RestoreObject'
//         }, load, pre, restore);

//         return server;
//     }

// };
