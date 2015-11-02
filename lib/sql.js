/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var once = require('once');

var pgError = require('./pg').pgError;
var control = require('./control');


///--- Handlers

function execSql(req, cb) {
    var pg = req.pg;
    var log = req.log;
    cb = once(cb);

    log.debug({
        stmt: req.stmt,
        values: req.values
    }, 'execSql: begin');
    var q = pg.query(req.stmt, req.values);

    q.on('row', function (r) {
        req.res.write(r);
    });

    q.once('error', cb);
    q.once('end', cb.bind(null, null));
}


function sql(options) {
    control.assertOptions(options);
    var route = 'sql';

    function _sql(stmt, values, opts, res) {
        var req = control.buildReq(opts, res, options);
        req.stmt = stmt;
        req.values = values;

        req.log.debug({
            stmt: stmt,
            values: values,
            opts: opts
        }, '%s: entered', route);

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee, {}, true),
                execSql
            ]
        });
    }

    return (_sql);
}



///--- Exports

module.exports = {
    sql: sql
};
