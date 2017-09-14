/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var dtrace = require('dtrace-provider');



///--- Globals

var DTraceProvider = dtrace.DTraceProvider;

var PROBES = {
    // msgid, req_id, bucket, key, value
    'putobject-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid, req_id
    'putobject-done': ['int', 'char *'],

    // msgid, req_id, bucket, fields, filter
    'update-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid, req_id
    'update-done': ['int', 'char *'],

    // msgid, req_id, bucket, filter
    'delmany-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, req_id
    'delmany-done': ['int', 'char *'],

    // msgid, req_id
    'batch-start': ['int', 'char *'],
    // msgid, req_id, bucket, op, (key || filter)
    'batch-op-start': ['int', 'char *', 'char *', 'char *', 'char *'],
    'batch-op-done': ['int', 'char *'],
    'batch-done': ['int', 'char *'],

    // msgid, req_id, bucket, key
    'getobject-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, value
    'getobject-done': ['int', 'char *'],

    // msgid, req_id, bucket, key
    'delobject-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, req_id
    'delobject-done': ['int', 'char *'],

    // msgid, req_id, bucket, filter
    'findobjects-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, key, id, etag, value
    'findobjects-record': ['int', 'char *', 'int', 'char *', 'char *'],

    // msgid, result_count
    'findobjects-done': ['int', 'int'],

    // msgid, req_id, bucket, count
    'reindexobjects-start': ['int', 'char *', 'char *', 'char *'],
    // msgid, bucket, key
    'reindexobjects-record': ['int', 'char *', 'char *'],
    // msgid, req_id
    'reindexobjects-done': ['int', 'char *'],

    // reqid, sql
    'query-start': ['char *', 'char *'],

    // reqid, sql
    'query-row': ['char *', 'char *', 'json'],

    // reqid, sql, error message
    'query-error': ['char *', 'char *', 'char *'],

    // reqid, sql
    'query-timeout': ['char *', 'char *'],

    // reqid, sql
    'query-done': ['char *', 'char *', 'json'],

    // msgid, request_type, handler_name, req_id
    'handler-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, req_id, route, handler_name, error message
    'handler-error': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid, request_type, handler_name
    'handler-done': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'getbucket-start': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket, found
    'getbucket-done': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'createbucket-start': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'createbucket-done': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'delbucket-start': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'delbucket-done': ['int', 'char *', 'char *'],

    // msgid, req_id
    'listbuckets-start': ['int', 'char *'],

    // msgid, req_id
    'listbuckets-done': ['int', 'char *'],

    // msgid, req_id, bucket
    'updatebucket-start': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'updatebucket-done': ['int', 'char *', 'char *'],

    // msgid, req_id, deep
    'ping-start': ['int', 'char *', 'int'],

    // msgid, req_id, deep
    'ping-done': ['int', 'char *', 'int']
};
var PROVIDER;



///--- API

module.exports = function exportStaticProvider() {
    if (!PROVIDER) {
        PROVIDER = dtrace.createDTraceProvider('moray');

        PROVIDER._fast_probes = {};

        Object.keys(PROBES).forEach(function (p) {
            var args = PROBES[p].splice(0);
            args.unshift(p);

            var probe = PROVIDER.addProbe.apply(PROVIDER, args);
            PROVIDER._fast_probes[p] = probe;
        });

        PROVIDER.enable();
    }

    return (PROVIDER);
}();
