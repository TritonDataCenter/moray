---
title: Moray: Joyent's Key/Value Store.
markdown2extras: tables, code-friendly
apisections: Buckets, Objects
---

# Moray

This is the reference documentation for Moray, which is a key/value store built
on top of [Manatee](https://mo.joyent.com/docs/manatee/master/), and
[fast](https://github.com/mcavage/node-fast).

This documentation provides descriptions of the APIs that Moray offers, as well
as how to use the (node.js) [client SDK](https://mo.joyent.com/node-moray) to
interact with Moray.

## Conventions

Any content formatted like this:

    $ getbucket foo

is a command-line example that you can run from a shell. All other examples and
information are formatted like this:

    client.createBucket(foo, {}, function (err) {
        assert.ifError(err);
    });

# Overview

Moray allows you to store arbitrary data as a JSON document in a `bucket`. A
bucket is essentially a namespace, such that bucket `foo` can have a key `mykey`
and bucket `bar` can have a key `mykey` with different values.  There is no
limit on the number of buckets, nor on the number of keys in a bucket.  That
said, a single Moray instance is backed by a single logical Postgres instance,
so you are practically limited to how much data you can maintain.  The basic
operations on objects in a bucket are `put`, `get`, and `delete`.

Upon creating a bucket you are allowed to define the bucket to have indexes,
which allow you to later `search` for multiple records that match those
indexes.  If indexes are defined on a bucket, when you write a key/value pair,
the value is automatically indexed server-side in Moray for you.  Indexes can
be defined to be of type `number`, `boolean`, `string`, or `inet` (IP address
or network). They can optionally be defined to enforce uniqueness of a value.

## Arrays

As of git commit `#ccb80c9`, Moray now supports the ability to define
multi-valued entries such that indexing still works (mostly) as expected.  A
small example is given, and then explained:

    var assert = require('assert-plus');
    var moray = require('moray');

    var cfg = {
        index: {
            name: {
                type: '[string]'
            }
        }
    };
    var client = moray.createClient({...});
    client.putBucket('foo', cfg, function (bucket_err) {
        assert.ifError(bucket_err);

        var data = {
            something_irrelevant: 'blah blah',
            name: ['foo', 'bar', 'baz']
        };
        client.putObject('foo', 'bar', data, function (put_err) {
            assert.ifError(put_err);

            var req = client.findObjects('foo', '(name=bar)');
            req.once('error', assert.ifError.bind(assert));
            req.on('record', ...);
            req.once('end', ...);
        });
    });

Array types work just like regular types, except they are defined by `[:type:]`,
and on writes, moray knows how to index those properly.  On searches, the
contract is to return the record if _any_ of the array values match the filter
subclause.  There is one caveat:  _wildcard_ searches do not work (and can't).
So doing `(name=f*)` will return an error to you.  The reason is that Postgres
does not have any sane way of doing this (it is technically possible, but
expensive, and not currently implemented in Moray).

## Triggers

While a quasi-advanced topic, Moray does support `pre` and `post` "triggers" on
buckets, which are guaranteed to run before and after a write
(so put|del object) is performed.  You write them as just plain old JS functions
when you create your bucket, and note that they are guaranteed to run as part of
the transaction scope.  You typically will use `pre` triggers to transform
and/or validate data before being saved, and use `post` triggers to write an
audit log or some such thing (UFDS leverages `post` triggers to generate the
changelog).  The are guaranteed to run in the order you define them.  Note that
`pre` triggers only run before `putObject`, whereas `post` runs after both
`putObject` and `delObject`.

In general, you need to be super careful with these, as they run in the same VM
that Moray does, so a null pointer deref will bring down the whole server.  This
is intentional for performance reasons.  So basically, don't screw up.

### pre

The definition for a pre trigger looks like this:

    function myPreTrigger(req, cb) {
        ....
        cb();
    }

Where `req` is an object like this:

    {
        bucket: 'foo',
        key: 'bar',
        log: <logger>,
        pg: <postgres handle>,
        schema: <index configuration>,
        value: {
            email: 'mark.cavage@joyent.com'
        }
    }

That is, you are passed in the name of the bucket, the name of the key, the
value (as a JS object) the user wanted to write, the bucket configuration, a
bunyan instance you can use to log to the Moray log, and the current Postgres
connection (which is a raw [node-pg](https://github.com/brianc/node-postgres/)
handle).

### post

The definition for a post trigger is nearly identical:


    function myPostTrigger(req, cb) {
        ....
        cb();
    }

Where `req` is an object like this:

    {
        bucket: 'foo',
        key: 'bar',
        id: 123,
        log: <logger>,
        pg: <postgres handle>,
        schema: <index configuration>,
        value: {
            email: 'mark.cavage@joyent.com'
        }
    }

The `req` object is basically the same, except you will now also have the
database `id` (a monotonically increasing sequence number) tacked in as well,
should you want to use that for something.

# Basic MorayClient usage

In order to interact with Moray, you are assumed to be using the `node-moray`
package, which you can install like:

    $ npm install git+ssh://git@github.com:joyent/node-moray.git

In order to then use it, here's some sample code:

    var bunyan = require('bunyan');
    var moray = require('moray');

    var client = moray.createClient({
        host: '127.0.0.1',
        port: 2020,
        log: bunyan.createLogger({
            name: 'moray',
            level: 'INFO',
            stream: process.stdout,
            serializers: bunyan.stdSerializers
        })
    });

    client.on('error', function (err) {
         // client is no longer usable
         // ...
    });

    client.on('connect', function () {
        client.close();
    });

Note that connect timeout and backoff/retry are options that are currently not
supported, but are on the (very) short-term TODO list.

All of the APIs shown below have an optional `options` argument that is always
second to last (callback being last). Typically, you would use this to pass in
a `req_id` that we can use to correlate logs from one service to the
interactions of moray.  Some APIs (namely put/get/del object) have additional
logic there to allow cache bypassing, etc.  For example, both of these are
valid:

    client.getObject('foo', 'bar', function (err, obj) {
        ...
    });

    var opts = {
        req_id: 1,
        noCache: true
    };
    client.getObject('foo', 'bar', opts, function (err, obj) {
        ...
    });

# Buckets

## CreateBucket

Creates a new bucket given a name and a `config` object.  The `config` object
defines an `index` section and optionally a `pre`, `post` array of trigger
functions.  Also, note that buckets can be versioned, such that subsequent
updates will fail if the version rolls backwards.

### API

 A "fully loaded" config (w/o post triggers) would look like this:

    var cfg = {
        index: {
            email: {
                type: "string",
                unique: true
            },
            userid: {
                type: "number",
                unique: true
            },
            ismanager: {
                type: "boolean"
            }
        },
        pre: [
            function enforceEmail(req, cb) {
                if (!req.value.email)
                    return (cb(new Error('email is required')));

                return (cb());
            },
            function setUID(req, cb) {
                req.value.userid = Math.floor(Math.random() * 100001);
                cb();
            }
        ],
        options: {
            version: 1
        }
    }

    client.createBucket('foo', cfg, function (err) {
        assert.ifError(err);
    });

### Inputs

| Field    | Type     | Description                                     |
| -------- | -------- | ----------------------------------------------- |
| name     | string   | globally unique name for the bucket             |
| config   | object   | configuration (indexes/pre/post) for the bucket |
| options  | object   | any optional parameters (req\_id)               |
| callback | function | only argument is `err`                          |

### Errors

* `InvalidBucketConfigError`
* `InvalidBucketNameError`
* `InvalidIndexDefinitionError`
* `NoDatabaseError`
* `NotFunctionError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

To add that config above, you can use this invocation (`-s says wait for stdin`):

    $ putbucket -s foo < test.json

Or if you're not using triggers (more common, actually):

    $ putbucket -u email -u userid:number -i ismanager:boolean foo

Note there is no `createbucket` command; `putbucket` wraps up `CreateBucket` and
`UpdateBucket`.

## GetBucket

Returns the configuration for the bucket only.  There really are no options here
besides the name of the bucket you want to fetch (and of course request\_id).

### API

     client.getBucket('foo', function (err, bucket) {
        assert.ifError(err);
        console.log(util.inspect(bucket));
    });

### Inputs

| Field    | Type     | Description                         |
| -------- | -------- | ----------------------------------- |
| name     | string   | globally unique name for the bucket |
| options  | object   | any optional parameters (req\_id)   |
| callback | function | only argument is `err`              |

### Errors

* `BucketNotFoundError`
* `NoDatabaseError`

### CLI

    $ getbucket foo

    {
      "name": foo",
      "index": {
        "email": {
          "type": "string",
          "unique": true
        },
        "userid": {
          "type": "number",
          "unique": true
        },
        "ismanager": {
          "type": "boolean"
        }
      },
      "pre": [
        "function enforceEmail(req, cb) {
           if (!req.value.email) {
             return (cb(new Error('email is required')));
           }
           return (cb());
         }",
        "function setUID(req, cb) {
          req.value.userid = Math.floor(Math.random() * 100001);
          cb();
        }"
      ],
      "post": [],
      "mtime": "2012-07-31T19:43:32.268Z"
    }

Note the `\n's` inside the `pre` functions were inserted for readability in this
document; they were not actually returned, and are not legal JSON.

## ListBuckets

Returns the configuration for all buckets.

### API

     client.listBuckets(function (err, buckets) {
        assert.ifError(err);
        console.log(util.inspect(buckets));
    });

### Inputs

| Field    | Type     | Description                       |
| -------- | -------- | --------------------------------- |
| options  | object   | any optional parameters (req\_id) |
| callback | function | only argument is `err`            |

### Errors

* `NoDatabaseError`

### CLI

    $ listbuckets

    [{
      "name": foo",
      "index": {
        "email": {
          "type": "string",
          "unique": true
        },
        "userid": {
          "type": "number",
          "unique": true
        },
        "ismanager": {
          "type": "boolean"
        }
      },
      "pre": [
        "function enforceEmail(req, cb) {
           if (!req.value.email) {
             return (cb(new Error('email is required')));
           }
           return (cb());
         }",
        "function setUID(req, cb) {
          req.value.userid = Math.floor(Math.random() * 100001);
          cb();
        }"
      ],
      "post": [],
      "mtime": "2012-07-31T19:43:32.268Z"
    }]

Note the `\n's` inside the `pre` functions were inserted for readability in this
document; they were not actually returned, and are not legal JSON.

## UpdateBucket

Updates a bucket given a name and a `config` object.  Basically, looks exactly
like `CreateBucket`.  The only reason we have `create|update bucket` and not
`putBucket` is so that users can selectively _not_ stomp all over a bucket if
they don't intend to (note there is a `putBucket` API in the client).

The `version` param in options is required if you are using versioned buckets,
and an error will be thrown if the version you are sending is `<=` than the
version in the database.

Note that if you *add* new indexes via `updateBucket`, then any *new* data will
be indexed accordingly, but _old_ data will not.  See the ReindexObjects
function for a method to update old rows. (A bucket version _must_ be specified
for `reindexObjects` to operation properly.)

Also, note that all operations involving the bucket configuration in Moray will
use a cached copy of the bucket configuration, so it may take a few minutes for
your update to take effect.  This caching can be overridden during subsequent
operations by setting the `noBucketCache` option to true.

### API

    var cfg = {
        index: {
            email: {
                type: "string",
                unique: true
            },
            userid: {
                type: "number",
                unique: true
            }
        },
        pre: [],
        post: []
    }

    client.updateBucket('foo', cfg, function (err) {
        assert.ifError(err);
    });

Or alternatively:

    client.putBucket('foo', cfg, function (err) {
        assert.ifError(err);
    });

### Inputs

| Field    | Type     | Description                                     |
| -------- | -------- | ----------------------------------------------- |
| name     | string   | globally unique name for the bucket             |
| config   | object   | configuration (indexes/pre/post) for the bucket |
| options  | object   | any optional parameters (req\_id)               |
| callback | function | only argument is `err`                          |

### Errors

* `BucketNotFoundError`
* `InvalidBucketConfigError`
* `InvalidIndexDefinitionError`
* `NoDatabaseError`
* `NotFunctionError`

### CLI

    $ putbucket -s foo < test.json
    $ putbucket -u email -u userid:number foo

## DeleteBucket

Deletes a bucket, *and all data in that bucket!* No real options to speak of.

### API

     client.delBucket('foo', function (err) {
        assert.ifError(err);
    });

### Inputs

| Field    | Type     | Description                         |
| -------- | -------- | ----------------------------------- |
| name     | string   | globally unique name for the bucket |
| options  | object   | any optional parameters (req\_id)   |
| callback | function | only argument is `err`              |

### Errors

* `BucketNotFoundError`
* `NoDatabaseError`

### CLI

    $ delbucket foo

# Objects

With the exception of `ReindexObjects`, the object-related methods in Moray use
a cache for bucket information.  When a bucket is updated, say to alter its
index columns, stale data will persist in the bucket cache for up to 300
seconds.  For object operations that absolutely require current bucket
information, setting the `noBucketCache` option will bypass that cache.

## PutObject

Creates or overwrites an object given a bucket, key and value.  The bucket must
obviously already exist, and the key can be any string at all.  The value must
be a JS object of any form that can be run through `JSON.stringify()`.  Any keys
in the object that match the names of those defined in the bucket's `index`
section are indexed, and available for searching later.

As far as options, besides the usual `req_id`, notably you can set the flag
`etag`, which will make moray _atomically_ test/set your new value (this is
useful if you're implementing something like a lock semantic).  To ensure that
you're "creating" a record that never existed, set etag to `null`, otherwise set
it to the value of the last known version of `etag` you have.  If the etag differs
you'll receive an `EtagConflictError` which, in addition to your being able to
switch on the name, is decorated with the info you need; a sample `.toString()`
looks like:

    {
        name: 'EtagConflictError',
        message: '...',
        context: {
            bucket: 'moray_unit_test_2423db7',
            key: '4ddb630b-cc3d-4ff7-bbb4-168cd005cbc0',
            expected: 'ABCDEFGH',
            actual: 'B21548FF'
        }
    }

### API

    var data = {
        email: 'mark.cavage@joyent.com',
        ismanager: false
    };
    client.putObject('foo', 'mcavage', data, function (err) {
        assert.ifError(err);
    });

### Inputs

| Field    | Type     | Description                             |
| -------- | -------- | --------------------------------------- |
| bucket   | string   | bucket to write this key in             |
| key      | string   | bucket to write this key in             |
| value    | object   | free-form JS object                     |
| options  | object   | any optional parameters (req\_id, etag) |
| callback | function | only argument is `err`                  |

### Errors

* `BucketNotFoundError`
* `EtagConflictError`
* `NoDatabaseError`
* `UniqueAttributeError`
* `InvalidIndexTypeError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

    $ putobject -d '{"email": "mark.cavage@joyent.com"}' foo mcavage

## GetObject

Retrieves an object by bucket/key.  An important option to keep in mind is the
`noCache` option.  By default, Moray keeps a fairly small in-memory cache for
`GetObject` specifically, so if you want strong consistency on reads, set
`noCache: true` in the client.  Note also that if noCache is not set, Moray will
try to read from the Postgres asynchronous slave.  So, if you want guaranteed read
after write consistency, set `noCache`.  This can be done on a request by
request basis.

Also, another important note is regarding the magic column `_txn_snap`.  This
field represents the postgres internal transaction *snapshot* that corresponds
to when this record was written (specifically the `xmin` component).  To really
get this, you probably want to go read up on how
[MVCC is implemented in postgres](http://momjian.us/main/writings/pgsql/internalpics.pdf);
specifically pp56-58.  The short of it is, for records in moray, it is
possible/probable that more than one record will have the same `_txn_snap`
value, so you cannot rely on it as a unique id.  However, you can rely on it to
be the only thing that will increase over time, and if your query's `_txn_snap`
is not the *latest* one, you won't see phantom reads.  This field was built into
Moray to solve the "sliding window" problem where consumers use Moray as a queue
(it makes sense if you want durable H/A...); what it means is that consumers
doing this need to use both the `_id` and `_txn_snap` fields.

### API

    client.getObject('foo', 'mcavage', function (err, obj) {
        assert.ifError(err);
        console.log(util.inspect(obj));
    });

Would output an object that has the attributes `bucket`, `key`, `value`, `_id`,
`_etag`, `_mtime` (in epoch time) and `_txn_snap`.  If all you care about is the
original user-set value, just use `obj.value`.  See the example CLI output below
for a full example.

### Inputs

| Field    | Type     | Description                                |
| -------- | -------- | ------------------------------------------ |
| bucket   | string   | bucket to write this key in                |
| key      | string   | bucket to write this key in                |
| options  | object   | any optional parameters (req\_id, noCache) |
| callback | function | arguments of `err` and `obj`               |

### Errors

* `BucketNotFoundError`
* `ObjectNotFoundError`
* `NoDatabaseError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

    $ getobject foo mcavage

    {
      "bucket": "foo",
      "key": "mcavage",
      "value": {
        "email": "mark.cavage@joyent.com",
        "userid": 54408
       },
       "_id": 1,
       "_etag": "D4DC3CFE",
       "_mtime": 1343724604354,
       "_txn_snap": 8349
    }

## FindObjects

Allows you to query a bucket for a set of records that match indexed fields.
Note this is not a streaming API (there is no pagination. do that yourself if
you want it).

Search filters are fully specified according to LDAP search filter rules (which
is one of the few sane parts of LDAP).  Whatever you search on must be part of
the index on the bucket config; this is non-negotiable.

In addition to the search filter, you can specify `limit`, `offset`, and `sort`;
the first two act like they usually do in DBs, and `sort` must be a JS object
specifying the attribute to sort by, and an order, which is one of `ASC` or
`DESC` (so also like DBs).  The default `limit` setting is `1000`. There is
no default `offset`.  The default `sort` order is `ASC`, and the default
attribute is `_id` (which means records are returned in the order they were
*created*).

### API

Note that the client API is implemented as an `EventEmitter`, as you should
expect to receive back up to N records from this call.

    var opts = {
        sort: {
           attribute: 'userid',
           order: 'DESC'
        }
    };
    var req = client.findObjects('foo', '(email=*@joyent.com)', opts);

    req.once('error', function (err) {
        console.error(err.stack);
        process.exit(1);
    });

    req.on('record', function (obj) {
        console.log(JSON.stringify(obj, null, 2));
    });

    req.on('end', function () {
       ...
    });


### Inputs

| Field   | Type   | Description                                            |
| ------- | ------ | ------------------------------------------------------ |
| bucket  | string | bucket to search in                                    |
| filter  | string | search filter string                                   |
| options | object | any optional parameters (req\_id, limit, offset, sort) |

### Errors

* `BucketNotFoundError`
* `InvalidQueryError`
* `NoDatabaseError`
* `NotIndexedError`
* `InvalidIndexTypeError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

    $ findobjects -d email foo "(email=*@joyent.com)"
    {
      "bucket": "foo",
      "key": "yunong",
      "value": {
        "email": "yunong@joyent.com",
        "userid": 74265
      },
      "_id": 4,
      "_etag": "09D27A3F",
      "_mtime": 1343726032177,
      "_txn_snap": 7894
    }
    {
      "bucket": "foo",
      "key": "mcavage",
      "value": {
        "email": "mark.cavage@joyent.com",
        "userid": 54408
      },
      "_id": 1,
      "_etag": "D4DC3CFE",
      "_mtime": 1343724604354,
      "_txn_snap": 7882
    }
    {
      "bucket": "foo",
      "key": "dap",
      "value": {
        "email": "dap@joyent.com",
        "userid": 23175
      },
      "_id": 3,
      "_etag": "84C510D7",
      "_mtime": 13437260263184,
      "_txn_snap": 7894
    }

## DeleteObject

Deletes an object by bucket/key.  Like `PutObject`, you can set an `etag` in
options to get `test/set` semantics.

### API

    var opts = {
        etag: 'D4DC3CFE'
    };
    client.delObject('foo', 'mcavage', opts, function (err) {
        assert.ifError(err);
    });

### Inputs

| Field    | Type     | Description                             |
| -------- | -------- | --------------------------------------- |
| bucket   | string   | bucket to delete this key from          |
| key      | string   | key to delete                           |
| options  | object   | any optional parameters (req\_id, etag) |
| callback | function | only argument is `err`                  |

### Errors

* `BucketNotFoundError`
* `EtagConflictError`
* `ObjectNotFoundError`
* `NoDatabaseError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

    $ delobject foo mcavage


## UpdateObjects

Allows you to bulk update a set of objects in one transaction.  Note you can
only update indexed fields.  You call this API with a bucket, a list of fields
to update and a filter, that is exactly the same syntax as `findObjects`.  To
use this API, you must set the bucket options `syncUpdates: true` at
`putBucket` time, otherwise you'll effectively have data corruption.

A few caveats:

- All objects affected by the update will have the same `_etag`.
- The `JSON value` will *not* be updated.  You will have to pay on every get
  request to merge them (the bucket options field drives this logic).

### API

    var attrs = {
        isManager: true
    };
    var filter = '(email=*@joyent.com)';
    var req = client.updateObjects('foo', attrs, filter, function (err, meta) {
        assert.ifError(err);
        console.log(meta.etag); // new etag for all updated objects
        ...
    });


### Inputs

| Field   | Type   | Description                                            |
| ------- | ------ | ------------------------------------------------------ |
| bucket  | string | bucket to write this key in                            |
| fields  | object | key/values to update                                   |
| filter  | string | search filter string                                   |
| options | object | any optional parameters (req\_id, limit, offset, sort) |

## ReindexObjects

After performing an `updateBucket` which added index fields, `reindexObjects`
can be used to automatically re-write objects so that all index fields are
properly populated.  This operation does _not_ update the `mtime` or `etag`
fields on stale objects when they are re-indexed.  Since it requires a `count`
parameter to set the max rows per iteration, `reindexObjects` must be called
repeatedly until it reports 0 rows processed.  Only then will the added indexes
be made available for use.

The selected `count` value should be chosen with the expected bucket object
size in mind.  Too large a value may cause Moray to consume excessive amounts
of memory since it's unable to exert backpressure on PostgreSQL when fetching
rows.

It is safe to make multiple simultaneous calls to `reindexObjects` acting on
the same bucket but it's likely to race on rows and incur rollbacks/slowdowns.

### API

    var rowsPerCall = 100;
    client.reindexObjects('foo', rowsPerCall, function (err, res) {
        assert.ifError(err);
        // rows processed in this call (0 if indexes are complete)
        console.log(res.processed);
        // rows remaining to reindex (absent of no_count option set)
        console.log(res.remaining);
    });


### Inputs

| Field   | Type   | Description                                  |
| ------- | ------ | -------------------------------------------- |
| bucket  | string | bucket to reindex                            |
| count   | object | max rows to reindex                          |
| options | object | any optional parameters (req\_id, no\_count) |

### Errors

* `BucketNotFoundError`
* `NoDatabaseError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

## DeleteMany

Allows you to bulk delete a set of objects in one transaction.  You call this
API with a bucket and a filter, that is exactly the same syntax as
`findObjects`.

### API

    var filter = '(email=*@joyent.com)';
    var req = client.deleteMany('foo', filter, function (err) {
        assert.ifError(err);
        ...
    });


### Inputs

| Field   | Type   | Description                                            |
| ------- | ------ | ------------------------------------------------------ |
| bucket  | string | bucket to delete from                                  |
| filter  | string | search filter string                                   |
| options | object | any optional parameters (req\_id, limit, offset, sort) |


## Batch

Allows you to transactionally write a list of put, update or delete operations.

If the type is `put`, each request creates or overwrites a list of
objects given a set of bucket/key/values.  The individual objects are
pretty much exactly what you would send to `putObject`, but in a list.
Note that all of the checks in putObject will be run, so you can send
in `etag` (notably) and enforcement will be done on an individual
record basis. Also, objects can span buckets in this API.

If the operation is `delete`, along with the object bucket/key, you can also
pass the same `opts` argument you do for `deleteObject`.

If the operation is `update`, you pass in what you would have to
`updateObjects`.

The default operation is `put`.

### API

    var data = [{
        bucket: 'foo',
        key: 'mcavage',
        value: {
            email: 'mark.cavage@joyent.com',
            ismanager: false
        }
    }, {
        bucket: 'foo',
        operation: 'update'
        fields: {
            email: 'mcavage@gmail.com'
        },
        filter: '(ismanager=false)'
    }, {
        bucket: 'bar',
        operation: 'delete'
        key: 'uniquething'
    }];
    client.batch(data, {req_id: '123'}, function (err, meta) {
        assert.ifError(err);
        console.log('etags: %j', meta.etags);
    });

### Inputs

| Field    | Type     | Description                                                       |
| -------- | -------- | ----------------------------------------------------------------- |
| objects  | object   | bucket/key/value tuples to store                                  |
| options  | object   | any optional parameters (req\_id, etag)                           |
| callback | function | only argument is `err` and `meta` which will have a list of etags |

### Errors

* `BucketNotFoundError`
* `EtagConflictError`
* `NoDatabaseError`
* `UniqueAttributeError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

# Other

## sql

An API that really only exists for two reasons:

1. for humans to quickly get some debug info from Moray
2. for systems like UFDS that need to create extra tables et al in Moray for
   `post` triggers.

### API

    var req = client.sql('select * from buckets_config');
    req.on('record', function (r) {
        console.log(JSON.stringify(r));
    });
    req.on('error', function (err) {
        ...
    });
    req.on('end', function () {
        ...
    });

### Inputs

| Field     | Type   | Description                          |
| --------- | ------ | ------------------------------------ |
| statement | string | SQL statement to run                 |
| values    | Array  | values to insert (see node-postgres) |
| options   | object | any optional parameters (req\_id)    |

### CLI

    $ sql "select now() from buckets_config;"
    {
        "now": "2012-08-01T15:50:33.291Z"
    }
