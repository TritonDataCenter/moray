// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var cluster = require('cluster');
var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var getopt = require('posix-getopt');
var extend = require('xtend');

var app = require('./lib');



///--- Globals

var DEFAULTS = {
        file: process.cwd() + '/etc/config.json',
        fork: true,
        logLevel: bunyan.WARN,
        port: 1234
};
var NAME = 'moray';
var LOG = bunyan.createLogger({
        name: NAME,
        level: (process.env.LOG_LEVEL || 'warn'),
        stream: process.stderr,
        serializers: {
                err: bunyan.stdSerializers.err,
                pg: function (client) {
                        return (client ? client._moray_id : undefined);
                }
        }
});



///--- Internal Functions

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('psvf:(file)', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'f':
                        opts.file = option.optarg;
                        break;

                case 'p':
                        opts.port = parseInt(option.optarg, 10);
                        break;

                case 's':
                        opts.fork = false;
                        break;

                case 'v':
                        // Allows us to set -vvv -> this little hackery
                        // just ensures that we're never < TRACE
                        LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
                        if (LOG.level() <= bunyan.DEBUG)
                                LOG = LOG.child({src: true});
                        break;

                default:
                        process.exit(1);
                        break;
                }
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
                process.exit(1);
        }

        return (extend({}, clone(DEFAULTS), cfg, options));
}


function run(options) {
        assert.object(options);

        var opts = clone(options);
        opts.log = LOG;
        opts.postgres.log = LOG;
        opts.name = NAME;

        var server = app.createServer(opts);

        server.on('error', function (err) {
                LOG.error({err: err}, 'server error');
                process.exit(1);
        });

        server.listen(options.port, function () {
                LOG.info('moray listening on %d', options.port);
        });
}


///--- Mainline
//
// Because everyone asks, the '_' here is because this is still in the global
// namespace.
//

var _config;
var _options = parseOptions();

LOG.debug({options: _options}, 'command line options parsed');
_config = readConfig(_options);
LOG.debug({config: _config}, 'configuration loaded');

if (cluster.isMaster && _config.fork) {
        var numCPUs = process.env.MORAY_THREADS || (os.cpus().length - 1 || 1);
        for (var i = 0; i < numCPUs; i++)
                cluster.fork();
} else {
        run(_config);
}
