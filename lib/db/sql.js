// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var util = require('util');

var args = require('../args');



///--- Globals

var sprintf = util.format;



///--- Helpers

/**
 * This trick courtesy of
 * http://blog.gvm-it.eu/post/7686382723/how-to-heredocs-in-javascript
 */
function heredoc(func) {
    var hd = func.toString();
    hd = hd.replace(/(^.*\{\s*\/\*\s*)/g, '');
    hd = hd.replace(/(\s*\*\/\s*\}.*)$/g, '');
    return hd;
}


function CHECK_CONFIG_EXISTS() { /*
    SELECT true FROM pg_tables WHERE tablename = 'buckets_config'
                                  */
}

function CREATE_CONFIG() { /*
    CREATE TABLE buckets_config (
        name TEXT PRIMARY KEY,
        schema TEXT NOT NULL,
        pre TEXT NOT NULL,
        post TEXT NOT NULL,
        ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
                                  */
}

function INSERT_CONFIG() { /*
    INSERT INTO buckets_config (name, schema, pre, post) VALUES ($1, $2, $3, $4)
                                   */
}


function UPDATE_CONFIG() { /*
    UPDATE buckets_config SET schema='%s', pre=$moray$%s$moray$, post=$moray$%s$moray$ WHERE name='%s'
                           */
}


function DELETE_CONFIG() { /*
    DELETE FROM buckets_config WHERE name = '%s'
                            */
}


function GET_BUCKET_CONFIG() { /*
    SELECT * FROM buckets_config WHERE name='%s'
                         */
}

function LIST_BUCKETS() { /*
    SELECT * FROM buckets_config
                           */
}


function DROP_ENTRY_COLUMN() { /*
    ALTER TABLE %s_entry DROP COLUMN %s
                         */
}


function DROP_TOMBSTONE_COLUMN() { /*
    ALTER TABLE %s_tombstone DROP COLUMN %s
                         */
}


function ADD_ENTRY_COLUMN() { /*
    ALTER TABLE %s_entry ADD COLUMN %s %s
                         */
}


function ADD_UNIQUE_ENTRY_COLUMN() { /*
    ALTER TABLE %s_entry ADD COLUMN %s %s UNIQUE
                         */
}


function ADD_TOMBSTONE_COLUMN() { /*
    ALTER TABLE %s_tombstone ADD COLUMN %s %s
                         */
}


function CREATE_ENTRY_TABLE() { /*
    CREATE TABLE %s_entry (
        id SERIAL,
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        etag CHAR(32) NOT NULL,
        mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP%s
     )
                                */
}


function CREATE_TOMBSTONE_TABLE() { /*
    CREATE TABLE %s_tombstone (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        etag CHAR(32) NOT NULL,
        dtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP%s
    )
                                */
}


function CREATE_ENTRY_INDEX() { /*
    CREATE INDEX %s_entry_%s_idx ON %s_entry(%s) WHERE %s IS NOT NULL
                           */
}


function DROP_ENTRY_TABLE() { /*
    DROP TABLE %s_entry
                              */
}


function DROP_TOMBSTONE_TABLE() { /*
    DROP TABLE %s_tombstone
                                  */
}


function COUNT_KEYS() { /*
    SELECT COUNT(key) FROM %s_entry WHERE key LIKE '%s%'
                         */
}


function LIST_KEYS() { /*
    SELECT key, etag, mtime, COUNT(*) OVER () FROM %s_entry
        WHERE key LIKE '%s%%' ORDER BY key LIMIT %d OFFSET %d
                        */
}


function SELECT_ENTRY_FOR_UPDATE() { /*
    SELECT * FROM %s_entry WHERE key='%s' FOR UPDATE
                                      */
}


function SELECT_TOMBSTONE_FOR_UPDATE() { /*
    SELECT * FROM %s_tombstone WHERE key='%s' FOR UPDATE
                                      */
}


function DELETE_TOMBSTONE() { /*
    DELETE FROM %s_tombstone WHERE key='%s'
                                      */
}


function TOMBSTONE_ENTRY() { /*
    WITH moved_rows AS (
        DELETE FROM %s_entry
        WHERE
            key='%s'
        RETURNING key, value, etag%s
    )
    INSERT INTO %s_tombstone (key, value, etag%s)
    SELECT * FROM moved_rows
                                      */
}


function INSERT_ENTRY() { /*
    INSERT INTO %s_entry (key, value, etag%s) VALUES ($1, $2, $3%s)
                           */
}


function GET_ENTRY() { /*
    SELECT id, key, value, etag, mtime FROM %s_entry WHERE key='%s'
                        */
}


function GET_TOMBSTONE_ENTRY() { /*
    SELECT key, value, etag, dtime FROM %s_tombstone WHERE key='%s'
                                 */
}


function DEL_AND_SAVE_ENTRY_ROWS() { /*
    DELETE FROM %s_entry WHERE key = '%s' RETURNING *
                                      */
}


function FIND_ENTRIES() { /*
    SELECT id, key, value, etag, mtime FROM %s_entry WHERE %s
                           */
}



///--- Exports

module.exports = {
    // This one is so annoying there's a wrapper
    createEntryIndexString: function createEntryIndexString(bucket, column) {
        assertString('bucket', bucket);
        assertString('column', column);

        var sql = module.exports.CREATE_ENTRY_INDEX;
        return sprintf(sql, bucket, column, bucket, column, column);
    }
};

//
// Here's some jack-hackery to avoid needing to add everything above to the
// exports. This is the equivalent of:
// module.exports: {
//    LIFBUCKETS: heredoc(LIST_BUCKETS),
//    ...
// }
//
// Basically, we read _this_ file in, line by line, and if the line is a
// function, we eval it into a reference, and tack the heredoc'd form onto
// module.exports.
//

fs.readFileSync(__filename, 'utf8').split('\n').forEach(function (l) {
    var match = /^function\s+(\w+)\(.*/.exec(l);
    if (match !== null && Array.isArray(match) && match.length > 1) {
        if (match[1] !== 'heredoc') {
            module.exports[match[1]] = heredoc(eval(match[1]));
        }
    }
});
