/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var VError = require('verror');


// --- Globals

var DEFAULTS = {
    file: process.cwd() + '/etc/config.json',
    monitorPort: 3020,
    port: 2020,
    bindip: '0.0.0.0'
};

var LOG_SERIALIZERS = {
    err: mod_bunyan.stdSerializers.err,
    pg: function (client) {
        return (client ? client._moray_id : undefined);
    }
};


// --- Exports

/**
 * Allows us to increase the verbosity with each additional -v without
 * ever dropping below the TRACE log level.
 */
function increaseVerbosity(log) {
    log.level(Math.max(mod_bunyan.TRACE, (log.level() - 10)));
    if (log.level() <= mod_bunyan.DEBUG)
        log = log.child({src: true});
    return log;
}

function readConfig(options) {
    assert.object(options);

    var cfg;

    try {
        cfg = JSON.parse(mod_fs.readFileSync(options.file, 'utf8'));
    } catch (e) {
        throw new VError(e,
            'Unable to parse configuration file %s', options.file);
    }

    return mod_jsprim.mergeObjects(cfg, options, DEFAULTS);
}

function setupLogger(name) {
    return mod_bunyan.createLogger({
        name: name,
        level: (process.env.LOG_LEVEL || 'info'),
        stream: process.stderr,
        serializers: LOG_SERIALIZERS
    });
}

module.exports = {
    LOG_SERIALIZERS: LOG_SERIALIZERS,

    increaseVerbosity: increaseVerbosity,
    readConfig: readConfig,
    setupLogger: setupLogger
};
