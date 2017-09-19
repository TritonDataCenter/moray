/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bsyslog = require('bunyan-syslog');
var bunyan = require('bunyan');
var clone = require('clone');
var extend = require('xtend');
var getopt = require('posix-getopt');
var jsprim = require('jsprim');
var VError = require('verror').VError;


var app = require('./lib');



///--- Globals

var MIN_PORT = 1;
var MAX_PORT = 65535;
var DEFAULTS = {
    file: process.cwd() + '/etc/config.json',
    monitorPort: 3020,
    port: 2020,
    bindip: '0.0.0.0'
};
var NAME = 'moray';
var LOG_SERIALIZERS = {
    err: bunyan.stdSerializers.err,
    pg: function (client) {
        return (client ? client._moray_id : undefined);
    }
};
//We'll replace this with the syslog later, if applicable
var LOG = bunyan.createLogger({
    name: NAME,
    level: (process.env.LOG_LEVEL || 'info'),
    stream: process.stderr,
    serializers: LOG_SERIALIZERS
});
var LOG_LEVEL_OVERRIDE = false;



///--- Internal Functions

function setupLogger(config) {
    var cfg_b = config.bunyan;
    assert.object(cfg_b, 'config.bunyan');
    assert.optionalString(cfg_b.level, 'config.bunyan.level');
    assert.optionalObject(cfg_b.syslog, 'config.bunyan.syslog');

    var level = LOG.level();

    if (cfg_b.syslog && !LOG_LEVEL_OVERRIDE) {
        assert.string(cfg_b.syslog.facility,
                      'config.bunyan.syslog.facility');
        assert.string(cfg_b.syslog.type, 'config.bunyan.syslog.type');

        var facility = bsyslog.facility[cfg_b.syslog.facility];
        LOG = bunyan.createLogger({
            name: NAME,
            serializers: LOG_SERIALIZERS,
            streams: [ {
                level: level,
                type: 'raw',
                stream: bsyslog.createBunyanStream({
                    name: NAME,
                    facility: facility,
                    host: cfg_b.syslog.host,
                    port: cfg_b.syslog.port,
                    type: cfg_b.syslog.type
                })
            } ]
        });
    }

    if (cfg_b.level && !LOG_LEVEL_OVERRIDE) {
        if (bunyan.resolveLevel(cfg_b.level))
            LOG.level(cfg_b.level);
    }
}


function parsePort(str) {
    var port = jsprim.parseInteger(str);

    if (port instanceof Error) {
        LOG.fatal({ port: str }, 'Invalid port');
        throw new VError(port, 'Invalid port %j', str);
    }

    if (port < MIN_PORT || port > MAX_PORT) {
        throw new VError('Invalid port %j: should be in range %d-%d',
            port, MIN_PORT, MAX_PORT);
    }

    return port;
}


function parseOptions() {
    var option;
    var opts = {};
    var parser = new getopt.BasicParser(':cvf:p:k:', process.argv);

    while ((option = parser.getopt()) !== undefined) {
        switch (option.option) {
        case 'c':
            opts.cover = true;
            break;
        case 'f':
            opts.file = option.optarg;
            break;

        case 'p':
            opts.port = parsePort(option.optarg);
            break;

        case 'k':
            opts.monitorPort = parsePort(option.optarg);
            break;

        case 'v':
            // Allows us to set -vvv -> this little hackery
            // just ensures that we're never < TRACE
            LOG_LEVEL_OVERRIDE = true;
            LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
            if (LOG.level() <= bunyan.DEBUG)
                LOG = LOG.child({src: true});
            break;

        case ':':
            throw new VError('Expected argument for -%s', option.optopt);

        default:
            throw new VError('Invalid option: -%s', option.optopt);
        }
    }

    if (parser.optind() !== process.argv.length) {
        throw new VError(
            'Positional arguments found when none were expected: %s',
            process.argv.slice(parser.optind()).join(' '));
    }

    if (!opts.file) {
        LOG.fatal({ opts: opts }, 'No config file specified.');
        throw new Error('No config file specified');
    }

    return (opts);
}


function readConfig(options) {
    assert.object(options);

    var cfg;

    try {
        cfg = JSON.parse(fs.readFileSync(options.file, 'utf8'));
    } catch (e) {
        LOG.fatal({
            err: e,
            file: options.file
        }, 'Unable to read/parse configuration file');
        throw new VError(e,
                         'Unable to parse configuration file %s',
                         options.file);
    }

    return (extend({}, clone(DEFAULTS), cfg, options));
}


function run(options) {
    assert.object(options);

    var opts = clone(options);
    opts.log = LOG;
    opts.name = NAME;

    var server = app.createServer(opts);
    server.listen();
}



///--- Mainline

(function main() {
    var options = parseOptions();
    var config = readConfig(options);

    LOG.debug({
        config: config,
        options: options
    }, 'main: options and config parsed');

    setupLogger(config);
    run(config);

    if (options.cover) {
        process.on('SIGUSR2', function () {
            process.exit(0);
        });
    }
})();
