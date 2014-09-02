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
var libuuid = require('libuuid');



///--- Handlers


function sql(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');

    var manatee = options.manatee;

    function _sql(stmt, values, opts, res) {
        var log = options.log.child({
            req_id: opts.req_id || libuuid.create()
        });

        log.debug({
            stmt: stmt,
            values: values,
            opts: opts
        }, 'sql: entered');

        manatee.start(function (startErr, pg) {
            if (startErr) {
                log.debug(startErr, 'sql: no DB handle');
                return (res.end(startErr));
            }

            var req = pg.query(stmt, values);

            req.once('error', function (err) {
                log.debug(err, 'postgres error');
                req.removeAllListeners('end');
                req.removeAllListeners('row');
                pg.rollback();
                res.end(err);
            });

            req.on('row', function (r) {
                log.debug(r, 'row received');
                res.write(r);
            });

            req.once('end', function () {
                log.debug('request done');
                req.removeAllListeners('error');
                req.removeAllListeners('row');
                pg.commit(function (err) {
                    res.end(err);
                });
            });
        });

    }

    return (_sql);
}



///--- Exports

module.exports = {
    sql: sql
};
