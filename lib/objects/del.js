// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');

var assert = require('assert-plus');
var libuuid = require('libuuid');
var vasync = require('vasync');

var common = require('./common');
var dtrace = require('../dtrace');
require('../errors');



///--- Globals

var sprintf = util.format;

// This is exported so that batch put can leverage it
var PIPELINE = [
    common.loadBucket,
    get_etag,
    drop,
    common.runPostChain
];

///--- Handlers

function get_etag(req, cb) {
    var pg = req.pg;
    var sql = sprintf('SELECT _etag, _id FROM %s WHERE _key = $1', req.bucket.name);
    var out_err;

    q = pg.query(sql, [ req.key ]);
    q.once('error', cb);
    q.once('row', function (r) {
      assert.string(r._etag, 'get_etag: r._etag');
      if (req.etag && r._etag !== req.etag) {
        out_err = new EtagConflictError(req.bucket.name,
          req.key, req.etag, r._etag);
      }
      assert.optionalNumber(r._id, 'get_etag: r._id');
      req.___old_id = r._id;
    });
    q.once('end', function () {
      cb(out_err);
    });
};

function drop(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    var sql = sprintf('DELETE FROM %s WHERE _key=$1', req.bucket.name);

    log.debug({
        bucket: req.bucket.name,
        key: req.key,
        sql: sql
    }, 'drop: entered');

    q = pg.query(sql, [req.key]);

    q.once('error', function (err) {
        log.debug(err, 'drop: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        if (!row && !req.___old_id) {
            log.debug('drop: record did not exist');
            cb(new ObjectNotFoundError(req.bucket.name, req.key));
        } else if (/* XXX */ false && req.etag && row._etag !== req.etag) {
            log.debug('drop: etag conflict');
            cb(new EtagConflictError(req.bucket.name,
                                     req.key,
                                     req.etag,
                                     row._etag));
        } else {
            if (req.___old_id)
                req._id = req.___old_id;
            else
                req._id = row._id;
            assert.ok(req._id);
            log.debug({id: req._id}, 'drop: done');
            cb();
        }
    });
}


function del(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _del(b, k, opts, res) {
        var id = opts.req_id || libuuid.create();

        dtrace['delobject-start'].fire(function () {
            return ([res.msgid, id, b, k]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'delObject: entered');

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'delObject: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'delObject: transaction started');

            vasync.pipeline({
                funcs: PIPELINE,
                arg: {
                    bucket: {
                        name: b
                    },
                    etag: (opts.etag || opts._etag),
                    key: k,
                    log: log,
                    pg: pg,
                    manatee: manatee,
                    headers: opts.headers || {},
                    req_id: id
                }
            }, common.pipelineCallback({
                log: log,
                name: 'delObject',
                pg: pg,
                res: res
            }));
        });
    }

    return (_del);
}



///--- Exports

module.exports = {
    del: del,
    pipeline: PIPELINE
};
