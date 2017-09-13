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
    'putobject-done': ['int'],

    // msgid, req_id, bucket, fields, filter
    'update-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid
    'update-done': ['int'],

    // msgid, req_id, bucket, filter
    'delmany-start': ['int', 'char *', 'char *', 'char *'],

    // msgid
    'delmany-done': ['int'],

    // msgid, req_id
    'batch-start': ['int', 'char *'],
    // msgid, req_id, bucket, op, (key || filter)
    'batch-op-start': ['int', 'char *', 'char *', 'char *', 'char *'],
    'batch-op-done': ['int'],
    'batch-done': ['int'],

    // msgid, req_id, bucket, key
    'getobject-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, value
    'getobject-done': ['int', 'char *'],

    // msgid, req_id, bucket, key
    'delobject-start': ['int', 'char *', 'char *', 'char *'],

    // msgid
    'delobject-done': ['int'],

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
    // msgid
    'reindexobjects-done': ['int'],

    // reqid, sql
    'query-start': ['char *', 'char *'],

    // reqid, sql
    'query-row': ['char *', 'char *', 'json'],

    // reqid, sql, error message
    'query-error': ['char *', 'char *', 'char *'],

    // reqid, sql
    'query-done': ['char *', 'char *', 'json'],

    // msgid, request_type, handler_name, req_id
    'handler-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, request_type, handler_name
    'handler-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'getbucket-start': ['int', 'char *', 'char *'],

    // msgid, reqid, name, sql, index
    'getbucket-row': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid, reqid, name, found
    'getbucket-done': ['int', 'char *', 'char *', 'int'],

    // msgid, reqid, bucket
    'createbucket-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql
    'createbucket-insertconfig-start': ['int', 'char *', 'char *'],

    // req_id, sql, error message
    'createbucket-insertconfig-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, req_id, bucket
    'createbucket-insertconfig-done': ['int', 'char *', 'char *'],

    // msgid, reqid, sql
    'createbucket-createsequence-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'createbucket-createsequence-error': ['char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'createbucket-createsequence-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'createbucket-createlockingserial-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'createbucket-createlockingserial-error': ['int', 'char *', 'char *',
        'char *'],

    // msgid, reqid, bucket
    'createbucket-createlockingserial-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'createbucket-createtable-start': ['int', 'char *', 'char *'],

    // msgid, sql, error message
    'createbucket-createtable-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'createbucket-createtable-done': ['int', 'char *', 'char *'],

    // msgid, reqid
    'loadbuckets-start': ['int', 'char *'],

    // msgid, reqid, row
    'loadbuckets-row': ['int', 'char *', 'json'],

    // msgid, reqid, sql
    'loadbuckets-error': ['int', 'char *', 'char *'],

    // msgid, reqid
    'loadbuckets-done': ['int', 'char *'],

    // msgid, reqid, bucket
    'delbucket-start': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-checkexists-start': ['int', 'char *', 'char *'],

    // msgid, sql, error message
    'delbucket-checkexists-error': ['int', 'char *', 'char *'],

    // msgid, reqid, row
    'delbucket-checkexists-row': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket, found
    'delbucket-checkexists-done': ['int', 'char *', 'char *', 'int'],

    // msgid, reqid, bucket
    'delbucket-deleteconfig-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'delbucket-deleteconfig-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-deleteconfig-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-droptable-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'delbucket-droptable-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-droptable-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-dropsequence-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'delbucket-dropsequence-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-dropsequence-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-droplockingserial-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'delbucket-droplockingserial-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'delbucket-droplockingserial-done': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket
    'updatebucket-loadbucket-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'updatebucket-loadbucket-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'updatebucket-loadbucket-done': ['int', 'char *', 'char *'],

    // msgid, reqid, row name
    'updatebucket-loadbucket-row': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'updatebucket-ensurereindexproperty-error': ['int', 'char *', 'char *',
        'char *'],

    // msgid, reqid, bucket
    'updatebucket-ensureindexproperty-done': ['int', 'char *', 'char *'],

    // msgid, req_id, bucket, diff
    'updatebucket-calculatediff-done': ['int', 'char *', 'char *', 'json'],

    // msgid, reqid, bucket
    'updatebucket-ensurerowver-start': ['int', 'char *', 'char *'],

    // msgid, reqid, error message
    'updatebucket-ensurerowver-error': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'updatebucket-updateconfig-start': ['int', 'char *', 'char *'],

    // msgid, reqid, sql, error message
    'updatebucket-updateconfig-error': ['int', 'char *', 'char *', 'char *'],

    // msgid, reqid, bucket
    'updatebucket-updateconfig-done': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'updatebucket-dropcolumns-start': ['int', 'char *', 'char *'],

    // msgid, reqid, error message
    'updatebucket-dropcolumns-error': ['int', 'char *', 'char *'],

    // msgid, reqid, bucket
    'updatebucket-addcolumns-start': ['int', 'char *', 'char *'],

    // msgid, reqid, error message
    'updatebucket-addcolumns-error': ['int', 'char *'],

    // msgid, reqid, bucket, indexes to add
    'updatebucket-createindexes-start': ['int', 'char *', 'char *', 'json'],

    // msgid, reqid, bucket, indexes to add
    'updatebucket-createuniqueindexes-start': ['int', 'char *', 'char *',
        'json']

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
