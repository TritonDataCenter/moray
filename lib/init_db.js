// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var vasync = require('vasync');

var pg = require('./pg');



///--- API

function init(options) {
        assert.object(options, 'options');


}



///--- Exports

module.exports = {
        init: init
};