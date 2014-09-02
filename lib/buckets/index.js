/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

function reexport(f) {
    var mod = require(f);
    Object.keys(mod).forEach(function (k) {
        module.exports[k] = mod[k];
    });
}

module.exports = {};
reexport('./creat');
reexport('./get');
reexport('./list');
reexport('./update');
reexport('./del');
