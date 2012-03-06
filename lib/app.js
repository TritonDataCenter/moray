// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');

var restify = require('restify');

var args = require('./args');
var buckets = require('./buckets');
var DB = require('./db');
var objects = require('./objects');



///--- Globals

var VERSION = false;

var assertArgument = args.assertArgument;



///--- Internal functions

/**
 * Returns the current semver version stored in CloudAPI's package.json.
 * This is used to set in the API versioning and in the Server header.
 *
 * @return {String} version.
 */
function version() {
    if (!VERSION) {
        var pkg = fs.readFileSync(__dirname + '/../package.json', 'utf8');
        VERSION = JSON.parse(pkg).version;
    }

    return VERSION;
}


/**
 * Reads and processes a configuration file.
 *
 * Overrides should be command line options (i.e., what you got from nopt).
 *
 * @arg {string} file      - config file to read.
 * @arg {object} overrides - command-line options.
 * @return {object} fully parsed configuration.
 * @throws {TypeError} on bad input.
 */
function configure(file, overrides) {
    assertArgument('file', 'string', file);
    assertArgument('overrides', 'object', overrides);

    var config;

    try {
        config = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (!config.port) {
            if (overrides.port) {
                config.port = overrides.port;
            } else {
                config.port = (config.certificate && config.key) ? 443 : 80;
            }
        }
    } catch (e) {
        console.error('Unable to parse %s: %s', file, e.message);
        process.exit(1);
    }

    try {
        if (config.certificate)
            config.certificate = fs.readFileSync(config.certificate, 'utf8');
    } catch (e) {
        console.error('Unable to load %s: %s', config.certificate, e.message);
        process.exit(1);
    }

    try {
        if (config.key)
            config.key = fs.readFileSync(config.key, 'utf8');
    } catch (e) {
        console.error('Unable to load %s: %s', config.certificate, e.message);
        process.exit(1);
    }

    if (!config.postgres) {
        console.error('Missing Postgres configuration from %s', file);
        process.exit(1);
    }

    config.dtrace = overrides.dtrace;
    config.log = overrides.log;
    config.log.level(config.logLevel);
    config.name = 'Moray ' + version();
    config.version = version();
    config.postgres.log = overrides.log;
    if (config.logLevel)
        config.log.level(config.logLevel);

    return config;
}



///--- API

module.exports = {

    /**
     * Wrapper over restify's createServer to make testing and
     * configuration handling easier.
     *
     * The returned server object will have a '.start()' method on it, which
     * wraps up the port/host settings for you.
     *
     * @arg {object} options      - options object.
     * @arg {string} options.file - configuration file to read from.
     * @arg {object} options.log  - bunyan logger.
     * @arg {function} callback   - of the form f(err, server).
     * @throws {TypeError} on bad input.
     */
    createServer: function createServer(options, callback) {
        assertArgument('options', 'object', options);
        assertArgument('options.log', 'object', options.log);
        assertArgument('callback', 'function', callback);

        var config = configure(options.file, options);

        var db = new DB(config.postgres);
        db.once('error', callback);
        db.on('connect', function () {
            db.removeListener('error', callback);

            var server = restify.createServer(config);
            server.use(restify.acceptParser(server.acceptable));
            server.use(restify.dateParser(config.maxRequestAge || 10));
            server.use(restify.queryParser());

            server.use(function addProxies(req, res, next) {
                req.db = db;
                return next();
            });

            buckets.mount(server);
            objects.mount(server);

            server.start = function start(cb) {
                return server.listen(config.port, cb);
            };

            return callback(null, server);
        });

    }

};
