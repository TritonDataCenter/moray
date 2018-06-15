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
 *   - "overlaps", "contains", "within", which describe how to handle the
 *     extended filters of the same name for each type, using the following
 *     attributes:
 *
 *     - "type", the name of the Moray type for handling the filter's
 *       argument
 *     - "op", the Postgres operation to apply; keep in mind that compileQuery()
 *       always moves the column name to the right side of the operator, so the
 *       filter "(attr>=5)" becomes "5 <= attr".
 *
 */

var TYPES = {
    'string': {
        array: false,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'TEXT',
        index: 'BTREE'
    },
    '[string]': {
        array: true,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'TEXT[]',
        index: 'GIN'
    },
    'number': {
        array: false,
        overlaps: null,
        contains: null,
        within: {
            type: 'numrange',
            op: '@>'
        },
        pg: 'NUMERIC',
        index: 'BTREE'
    },
    '[number]': {
        array: true,
        overlaps: null,
        contains: null,
        within: {
            type: 'numrange',
            op: '@>'
        },
        pg: 'NUMERIC[]',
        index: 'GIN'
    },
    'numrange': {
        array: false,
        overlaps: {
            type: 'numrange',
            op: '&&'
        },
        contains: {
            type: 'number',
            op: '<@'
        },
        within: null,
        pg: 'NUMRANGE',
        index: 'GIST'
    },
    'boolean': {
        array: false,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'BOOLEAN',
        index: 'BTREE'
    },
    '[boolean]': {
        array: true,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'BOOLEAN[]',
        index: 'GIN'
    },
    'date': {
        array: false,
        overlaps: null,
        contains: null,
        within: {
            type: 'daterange',
            op: '@>'
        },
        pg: 'TIMESTAMPTZ',
        index: 'BTREE'
    },
    '[date]': {
        array: true,
        overlaps: null,
        contains: null,
        within: {
            type: 'daterange',
            op: '@>'
        },
        pg: 'TIMESTAMPTZ[]',
        index: 'GIN'
    },
    'daterange': {
        array: false,
        overlaps: {
            type: 'daterange',
            op: '&&'
        },
        contains: {
            type: 'date',
            op: '<@'
        },
        within: null,
        pg: 'TSTZRANGE',
        index: 'GIST'
    },
    'ip': {
        array: false,
        overlaps: null,
        contains: null,
        within: {
            type: 'subnet',
            op: '>>'
        },
        pg: 'INET',
        /*
         * Once we move to a newer version of Postgres, we can change the
         * default index type here (and for "subnet") to GIST, as long as
         * we also make sure to specify the "inet_ops" operator class during
         * the CREATE INDEX.
         */
        index: 'BTREE'
    },
    '[ip]': {
        array: true,
        overlaps: null,
        contains: null,
        within: {
            type: 'subnet',
            op: '>>'
        },
        pg: 'INET[]',
        index: 'GIN'
    },
    'mac': {
        array: false,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'MACADDR',
        index: 'BTREE'
    },
    '[mac]': {
        array: true,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'MACADDR[]',
        index: 'GIN'
    },
    'subnet': {
        array: false,
        /*
         * Postgres 9.4 added support for the "&&" operator for the CIDR
         * type, but we currently have to always maintain compatibility
         * with Postgres 9.2. As soon as we can assume a higher minimum
         * version, we can update "overlaps" here.
         */
        overlaps: null,
        contains: {
            type: 'ip',
            op: '<<'
        },
        within: null,
        pg: 'CIDR',
        index: 'BTREE'
    },
    '[subnet]': {
        array: true,
        overlaps: null,
        contains: {
            type: 'ip',
            op: '<<'
        },
        within: null,
        pg: 'CIDR[]',
        index: 'GIN'
    },
    'uuid': {
        array: false,
        overlaps: null,
        contains: null,
        within: null,
        pg: 'UUID',
        index: 'BTREE'
    }
};

module.exports = {
    TYPES: TYPES
};
