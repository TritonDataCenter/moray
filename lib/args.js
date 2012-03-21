// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var sprintf = require('util').format;

require('./errors');
require('./patch');



///--- API

function assertArgument(name, type, arg) {
    var ok = false;

    if (Array.isArray(type)) {
        for (var i = 0; i < type.length; i++) {
            if (typeof (arg) === type) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            throw new InvalidArgumentError('%s ([%s]) required',
                                           name, type.join().capitalize());
        }
    } else {
        if (arg === undefined && type !== undefined) {
            throw new MissingParameterError('%s is required', name);
        } else if (typeof (arg) !== type) {
            throw new InvalidArgumentError('%s (%s) required',
                                           name, type.capitalize());
        }
    }
}


function assertArray(name, type, arr) {
    if (!Array.isArray(arr))
        throw new InvalidArgumentError('%s (Array) required', name);

    if (type !== null) {
        var ok = true;
        arr.forEach(function (i) {
            if (typeof (i) !== type) {
                ok = false;
            }
        });

        if (!ok) {
            throw new InvalidArgumentError('%s ([%s]) required',
                                           name, type.capitalize());
        }
    }
}


function assertBoolean(name, arg) {
    return assertArgument(name, 'boolean', arg);
}


function assertFunction(name, arg) {
    return assertArgument(name, 'function', arg);
}


function assertNumber(name, arg) {
    return assertArgument(name, 'number', arg);
}


function assertObject(name, arg) {
    return assertArgument(name, 'object', arg);
}


function assertString(name, arg) {
    return assertArgument(name, 'string', arg);
}



///--- Exports

module.exports = {

    assertArgument: assertArgument,
    assertArray: assertArray,
    assertBoolean: assertBoolean,
    assertFunction: assertFunction,
    assertNumber: assertNumber,
    assertObject: assertObject,
    assertString: assertString

};

Object.keys(module.exports).forEach(function (k) {
    global[k] = module.exports[k];
});
