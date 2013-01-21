// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

function reexport(f) {
        var mod = require(f);
        Object.keys(mod).forEach(function (k) {
                module.exports[k] = mod[k];
        });
}

module.exports = {};
reexport('./put');
reexport('./batch_put');
reexport('./get');
reexport('./del');
reexport('./find');
reexport('./update');
