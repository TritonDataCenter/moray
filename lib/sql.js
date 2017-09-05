/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var once = require('once');

var pgError = require('./pg').pgError;
var control = require('./control');


///--- Globals

var ARGS_SCHEMA = [
    { name: 'statement', type: 'string' },
    { name: 'values', type: 'array' },
    { name: 'options', type: 'options' }
];

var PIPELINE = [
    control.getPGHandleAndTransaction,
    execSql
];


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
        req.rpc.write(r);
    });

    q.once('error', cb);
    q.once('end', cb.bind(null, null));
}


function sql(options) {
    control.assertOptions(options);

    function _sql(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var stmt = argv[0];
        var values = argv[1];
        var opts = argv[2];

        var req = control.buildReq(opts, rpc, options);
        req.stmt = stmt;
        req.values = values;

        req.log.debug({
            stmt: stmt,
            values: values,
            opts: opts
        }, 'sql: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE
        });
    }

    return (_sql);
}



///--- Exports

module.exports = {
    sql: sql
};
