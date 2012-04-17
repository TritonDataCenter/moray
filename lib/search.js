// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var crypto = require('crypto');
var util = require('util');

var restify = require('restify');

var args = require('./args');
var errors = require('./errors');



///--- Globals

var sprintf = util.format;



///--- Routes

function buildFilterString(req, res, next) {
    if (req.params._filter) {
        assertString('_filter', req.params._filter);
        return next();
    }

    var q = [];
    Object.keys(req.query || {}).forEach(function (k) {
        var s = sprintf('(%s=%s)', k, req.query[k]);
        q.push(s);
    });

    switch (q.length) {
    case 0:
        return next(new MissingParameterError('no search filter specified'));

    case 1:
        req.params._filter = q[0];
        break;

    default:
        req.params._filter = sprintf('(&%s)', q.join(''));
        break;
    }

    return next();
}


function search(req, res, next) {
    var bucket = req.params.bucket;
    var db = req.objectManager;
    var filter = req.params._filter;
    var log = req.log;
    var opts = {
        sort: req.headers['x-moray-sort']
    };

    log.debug({
        bucket: req.params.bucket,
        filter: req.params._filter,
        opts: opts
    }, 'SearchBucket entered');
    return db.find(bucket, filter, opts, function (err, objects) {
        if (err)
            return next(err);

        var md5 = crypto.createHash('md5');
        md5.update(JSON.stringify(objects), 'utf8');
        res.etag = md5.digest('hex');
        res.send(objects);
        return next();
    });
}



///--- Exports

module.exports = {
    mount: function mount(server) {
        args.assertArgument(server, 'object', server);

        var pre = restify.conditionalRequest();

        server.get({path: '/search/:bucket', name: 'SearchBucket'},
                   pre, buildFilterString, search);
        server.head({path: '/search/:bucket', name: 'HeadSearchBucket'},
                    pre, buildFilterString, search);

        return server;
    }

};
