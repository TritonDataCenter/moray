// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var cluster = require('cluster');
var net = require('net');
var os = require('os');
var path = require('path');
var repl = require('repl');

var d = require('dtrace-provider');
var Logger = require('bunyan');
var nopt = require('nopt');
var restify = require('restify');

var app = require('./lib').app;


///--- Globals

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
        DTP.enable();

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
    for (var i = 0; i < os.cpus().length; i++)
        cluster.fork();

    cluster.on('death', function (worker) {
        LOG.error({worker: worker}, 'worker %d exited');
    });

    startREPL();
} else {
    run();
}

process.on('uncaughtException', function (err) {
    LOG.fatal({err: err}, 'uncaughtException: %s', err.stack);
    server.stop(function () {
        process.exit(1);
    });
});
