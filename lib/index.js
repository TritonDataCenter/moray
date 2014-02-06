// Copyright 2012 Joyent, Inc.  All rights reserved.

var server = require('./server');



///--- Exports

module.exports = {

    createServer: server.createServer,
    buckets: require('./buckets'),
    objects: require('./objects')

};
