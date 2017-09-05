/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

/*
 * All of the Moray types are defined in the object below. Each type has
 * several properties that define its behaviour:
 *
 *   - "array", whether this is an array type.
 *   - "pg", the name of the Postgres type that it maps to.
 *   - "index", the kind of Postgres index to use on the column.
 */

var TYPES = {
    'string': {
        array: false,
        pg: 'TEXT',
        index: 'BTREE'
    },
    '[string]': {
        array: true,
        pg: 'TEXT[]',
        index: 'GIN'
    },
    'number': {
        array: false,
        pg: 'NUMERIC',
        index: 'BTREE'
    },
    '[number]': {
        array: true,
        pg: 'NUMERIC[]',
        index: 'GIN'
    },
    'boolean': {
        array: false,
        pg: 'BOOLEAN',
        index: 'BTREE'
    },
    '[boolean]': {
        array: true,
        pg: 'BOOLEAN[]',
        index: 'GIN'
    },
    'ip': {
        array: false,
        pg: 'INET',
        index: 'BTREE'
    },
    '[ip]': {
        array: true,
        pg: 'INET[]',
        index: 'GIN'
    },
    'subnet': {
        array: false,
        pg: 'CIDR',
        index: 'BTREE'
    },
    '[subnet]': {
        array: true,
        pg: 'CIDR[]',
        index: 'GIN'
    }
};

module.exports = {
    TYPES: TYPES
};
