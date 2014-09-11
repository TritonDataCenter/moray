/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

require('../errors');
var control = require('../control');



///--- Handlers

function loadBuckets(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var rows = [];
    var sql = 'SELECT * FROM buckets_config';

    log.debug('loadBuckets: entered');

    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug(err, 'loadBuckets: failed');
        cb(err);
    });

    q.on('row', function (r) {
        r.options = r.options || {};
        r.options.version = r.options.version || 0;
        req.res.write(r);
    });

    q.once('end', function () {
        log.debug({
            buckets: rows
        }, 'loadBuckets: done');
        cb(null);
    });
}


function list(options) {
    control.assertOptions(options);
    var route = 'listBuckets';

    function _list(opts, res) {
        var req = control.buildReq(opts, res, options.log);

        control.handlerPipeline({
            route: route,
            req: req,
            funcs: [
                control.getPGHandle(route, options.manatee, {read:true}),
                loadBuckets
            ]
        });
    }

    return (_list);
}



///--- Exports

module.exports = {
    list: list
};
