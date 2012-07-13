// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var cluster = require('cluster');
var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var clone = require('clone');
var getopt = require('posix-getopt');
var restify = require('restify');
var extend = require('xtend');

var app = require('./lib');



///--- Globals

var DEFAULTS = {
        file: process.cwd() + '/etc/config.json',
        fork: true,
        logLevel: bunyan.WARN,
        port: 80
};
var NAME = 'moray';
var LOG = bunyan.createLogger({
        name: NAME,
        level: (process.env.LOG_LEVEL || 'warn'),
        stream: process.stderr,
        serializers: restify.bunyan.serializers
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
        var numCPUs = (os.cpus().length - 1 || 1);
        for (var i = 0; i < numCPUs; i++)
                cluster.fork();
} else {
        run(_config);
}



/*



var NAME = 'moray';
var DEFAULT_CFG = __dirname + '/etc/' + NAME + '.config.json';
var LOG;
var PARSED;
var DTP;
var SERVERS = [];

var OPTS = {
    'debug': Number,
    'file': String,
    'port': Number,
    'help': Boolean
};

var SHORT_OPTS = {
    'd': ['--debug'],
    'f': ['--file'],
    'p': ['--port'],
    'h': ['--help']
};



///--- Internal Functions

function usage(code, message) {
    var _opts = '';
    Object.keys(SHORT_OPTS).forEach(function (k) {
        var longOpt = SHORT_OPTS[k][0].replace('--', '');
        var type = OPTS[longOpt].name || 'string';
        if (type && type === 'boolean') type = '';
        type = type.toLowerCase();

        _opts += ' [--' + longOpt + ' ' + type + ']';
    });

    var msg = (message ? message + '\n' : '') +
        'usage: ' + path.basename(process.argv[1]) + _opts;

    console.error(msg);
    process.exit(code);
}


function run(callback) {
    var opts = {
        file: PARSED.file || DEFAULT_CFG,
        overrides: PARSED,
        log: LOG,
        dtrace: DTP
    };

    return app.createServer(opts, function (err, server) {
        if (err) {
            console.error('Unable to create server: %s', err.message);
            process.exit(1);
        }

        server.start(function () {
            SERVERS.push(server);
            LOG.info('%s listening at %s', NAME, server.url);
            if (typeof (callback) === 'function')
                return callback();
            return false;
        });
    });
}


function startREPL() {
    net.createServer(function (socket) {
        var r = repl.start('moray> ', socket);
        r.context.SERVERS = SERVERS;
    }).listen(5001, 'localhost', function () {
        LOG.info('REPL started on 5001');
    });
}



///--- Mainline

PARSED = nopt(OPTS, SHORT_OPTS, process.argv, 2);
if (PARSED.help)
    usage(0);

DTP = d.createDTraceProvider(NAME);
LOG = new Logger({
    level: PARSED.debug ? 'debug' : 'info',
    name: NAME,
    stream: process.stderr,
    serializers: {
        err: Logger.stdSerializers.err,
        req: Logger.stdSerializers.req,
        res: restify.bunyan.serializers.response
    }
});

if (PARSED.debug) {
    if (PARSED.debug > 1)
        LOG.level('trace');

    run(startREPL());
} else if (cluster.isMaster) {
    for (var i = 0; i < os.cpus().length - 1; i++)
        cluster.fork();

    cluster.on('death', function (worker) {
        LOG.error({worker: worker}, 'worker %d exited');
        cluster.fork();
    });

    startREPL();
} else {
    run();
}

process.on('uncaughtException', function (err) {
    LOG.fatal({err: err}, 'uncaughtException handler (exiting error code 1)');
    process.exit(1);
});
*/