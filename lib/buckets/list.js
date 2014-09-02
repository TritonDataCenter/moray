/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var deepEqual = require('deep-equal');
var libuuid = require('libuuid');
var vasync = require('vasync');

require('../errors');



///--- Globals

var sprintf = util.format;



///--- Handlers

function loadBuckets(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var rows = [];
    var sql = sprintf('SELECT * FROM buckets_config');

    log.debug('loadBuckets: entered');

    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'loadBuckets: failed');
        cb(err);
    });

    q.on('row', function (r) {
        rows.push(r);
    });

    q.once('end', function () {
        log.debug({
            buckets: rows
        }, 'loadBuckets: done');
        cb(null, rows);
    });
}


function list(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _get(opts, res) {
        var log = options.log.child({
            req_id: opts.req_id || libuuid.create()
        });

        log.debug({
            opts: opts
        }, 'listBuckets: entered');

        manatee.pg({read: true}, function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'listBuckets: no DB handle');
                res.end(startErr);
                return;
            }

            if (opts.timeout)
                pg.setTimeout(opts.timeout);

            log.debug({
                pg: pg
            }, 'listBuckets: client acquired');

            vasync.pipeline({
                funcs: [ loadBuckets ],
                arg: {
                    log: log,
                    pg: pg,
                    manatee: manatee
                }
            }, function (err, results) {
                if (err) {
                    log.warn(err, 'listBuckets: failed');
                    res.end(err);
                } else {
                    (results.successes || []).forEach(function (buckets) {
                        buckets.forEach(function (b) {
                            b.options = b.options || {};
                            b.options.version = b.options.version || 0;
                            res.write(b);
                        });
                    });
                    log.debug('listBuckets: done');
                    res.end();
                }
                pg.release();
            });
        });
    }

    return (_get);
}



///--- Exports

module.exports = {
    list: list
};
