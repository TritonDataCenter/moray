// Copyright 2012 Joyent, Inc.  All rights reserved.

var app = require('./app');
var DB = require('./db');

// Just invoke this, as it enhances a few prototypes
require('./patch');



///--- Exports

module.exports = {

    app: app,
    DB: DB

};
