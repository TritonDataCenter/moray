// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

function reexport(f) {
        var mod = require(f);
        Object.keys(mod).forEach(function (k) {
                module.exports[k] = mod[k];
        });
}

module.exports = {};
reexport('./put');
reexport('./batch');
reexport('./get');
reexport('./del');
reexport('./del_many');
reexport('./find');
reexport('./update');
