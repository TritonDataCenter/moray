// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var sprintf = require('util').format;



///--- API

module.exports = {

    assertArgument: function assertArgument(name, type, arg) {
        var msg;
        var ok = false;

        if (Array.isArray(type)) {
            for (var i = 0; i < type.length; i++) {
                if (typeof (arg) === type) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                msg = sprintf('%s ([%s]) required', name, type.join());
                throw new TypeError(msg);
            }
        } else {
            if (typeof (arg) !== type) {
                msg = sprintf('%s (%s) required', name, type.capitalize());
                throw new TypeError(msg);
            }
        }
    },


    assertArray: function assertArray(name, type, arr) {
        if (!Array.isArray(arr))
            throw new TypeError(sprintf('%s (Array) required', name));

        if (type !== null) {
            var ok = true;
            arr.forEach(function (i) {
                if (typeof (i) !== type)
                    ok = false;
            });

            if (!ok) {
                var msg =
                    sprintf('%s ([%s]) required', name, type.capitalize());
                throw new TypeError(msg);
            }
        }
    }

};
