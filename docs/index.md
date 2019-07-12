---
title: Moray: Joyent's Key/Value Store.
markdown2extras: tables, code-friendly
apisections: Buckets, Objects
---

# Moray

This is the reference documentation for Moray, which is a key-value store built
on top of [Manatee](https://github.com/joyent/manatee/) and
[fast](https://github.com/joyent/node-fast).  For general information about
Moray, see the [README](https://github.com/joyent/moray).

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

Moray allows you to store arbitrary JSON documents (called **objects**) inside
namespaces called **buckets**.  Buckets can have **indexes**, which are
top-level fields that are extracted so that search operations using those fields
can be executed efficiently.  Indexes can also be used to enforce uniqueness of
a value.  Since buckets are essentially namespaces, bucket `foo` can have a key
`mykey` and bucket `bar` can have a key `mykey` with different values.

There is no limit on the number of buckets, nor on the number of keys in a
bucket.  That said, a single Moray instance is backed by a single logical
Postgres instance, so you are practically limited to how much data you can
maintain.

The basic operations on objects in a bucket are `put`, `get`, and `delete`.
`search`, `update`, and `deleteMany` are also supported using filter strings
that operate on indexed fields.

You define a bucket's indexes when you initially create the bucket.  When you
write an object, the value of each indexed field is updated in the server-side
index for that field.  You can update a bucket's indexes later, but objects
already written in that bucket will need to be reindexed.

Indexes can be defined to be any of the following types:

- `number`, a numeric value (e.g., `5` or `123.456`)
- `boolean`, a boolean value (e.g., `true` or `false`)
- `string`, a string value (e.g., `"hello world"`)
- `uuid`, a UUID (e.g., `"3bfa93f9-012e-c14c-fc29-9042357e684d"`)
- `date`, an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date
  (e.g., `"2019-07-04T20:30:55.123Z"` or `"2019-07-04T14:30:00Z"`)
- `mac`, a 48-bit MAC address (e.g., `"90:b8:d0:e6:be:e1"`)
- `ip`, an IPv4 or IPv6 address (e.g., `"192.168.0.1"` or `"fd00::1"`)
- `subnet`, an IPv4 or IPv6 subnet (e.g., `"192.168.4.0/24"` or `"fc00::/7"`)
- `numrange`, a range of numbers (e.g., `"[5,10]"` or `"(5,9.5]"`)
- `daterange`, a range of dates (e.g., `"[,2019-07-04T00:00:00Z)"` or
  `"[2019-07-04T00:00:00Z,2019-07-05T00:00:00Z)"`)

For information on using range types, see [Using Ranges](#using-ranges).

Moray can also index fields containing arrays of the types listed above, with
some limitations in how they may be searched. See [Using Arrays](#using-arrays)
for more information.

Indexed field names must:

- Contain only Latin letters, Arabic numerals, and underscores.
- Start with a Latin letter or an underscore.
- Not end with an underscore.
- When starting with an underscore, not contain any further underscores.
- Not start with `moray`.
- Not be a reserved name (`_etag`, `_id`, `_idx`, `_key`, `_atime`, `_ctime`,
  `_mtime`, `_rver`, `_txn_snap`, `_value`, or `_vnode`).

## Filters

Moray allows searching for objects using a filter language similar to LDAP's. To
search on a field, you can use the following expressions:

- Presence, `(field=*)`, which applies to objects where `field` has been
  set and is non-null.
- Equals, `(field=value)`, which applies to objects where `field` is the
  given value.
- Less than or equals, `(field<=value)`, which applies to objects where the
  value of `field` is less than or equal to the given value.
- Greater than or equals, `(field>=value)`, which applies to objects where the
  value of `field` is greater than or equal to the given value.
- Substring, which applies to objects where `field` contains the given
  substrings; wildcard characters (`*`) may be placed within the string
  wherever they are needed, allowing variations like `(field=prefix*)`,
  `(field=*suffix)`, `(field=*infix*)`, `(field=*a*b*c*)`, and so on.

When using `<=` and `>=`, values are compared according to Postgres's rules for
that type. For strings, this is by codepoint value (`"Z"` is less than `"^"`,
which is less than `"a"`, which in turn is less than `"𐐗"`). When comparing a
string that is the proper prefix of another string, the prefix will sort before
the longer string.

Like in LDAP, you can join multiple filters to create more complex expressions:

- Or, `(|(...)(...))`
- And, `(&(...)(...))`
- Negation, `(!(...))`

Moray also supports the following extensible filters:

- `(field:caseIgnoreMatch:=value)`, which applies to objects where `field` is
  equal to `value` when ignoring case
- `(field:caseIgnoreSubstringsMatch:=*value*)`, which applies to objects where
  `field` contains the given string when ignoring case
- `(field:overlaps:=range)`, which applies to objects where the range-type
  `field` overlaps with the given range
- `(field:contains:=value)`, which applies to objects where the range-type
  `field` contains the given value
- `(field:within:=range)`, which applies to objects where `field` is contained
  within the given range

When constructing filters based on user input, it's important to make sure that
input is properly escaped. Unlike LDAP, which represents escaped characters as
their hexadecimal ASCII values (e.g., a literal `(` is `\28`), Moray escapes
the literal character (e.g., a literal `(` is `\(`). Consumers should consider
using the [moray-filter](https://www.npmjs.com/package/moray-filter) library to
make sure their queries escape unknown input correctly.

Moray requires that at least one of the filters in a query be for an indexed
field, so that Postgres always has the option to use its indexes. If a filter
is given for a non-indexed field (or a field that is currently being indexed by
[ReindexObjects](#reindexobjects)), then Moray will apply the filter itself to
results returned from Postgres. While this can sometimes be a useful feature,
it can also be extremely tricky for consumers to use safely. When filtering on
fields without an index:

- The `_count` field will likely be incorrect, since it represents the number
  of rows that match the query sent to Postgres.
- Zero objects may be returned even though there might be matching objects in
  the bucket, since the [`limit` option](#option-limit) constrains the number
  of objects returned from the database, which may then all be removed by the
  filter on the non-indexed field. While using [`noLimit`](#option-limit) can
  fix this issue, it introduces its own, more severe issues when dealing with
  large tables.
- Moray assumes non-indexed fields are strings, which means that other field
  types cannot be reliably matched; for example, if a field on an object is
  boolean `true`, and the filter is `(field=true)`, the object will not be
  returned since `true` is not the same as `"true"`.

To avoid these kinds of issues, it is recommended that consumers only use fully
indexed fields. The [`requireIndexes` options](#option-requireIndexes) can be
used to tell Moray to reject filters that include non-indexed or reindexing
fields.

In addition to the fields specified in the bucket's schema, you can search and
[sort](#option-sort) on the following internal fields:

- `_id`, an ID that Moray assigns to each row that is unique within the bucket
- `_key`, the key of the object
- `_mtime`, the modification time of the object

# Basic MorayClient usage

You can install the node-moray client library and CLI tools using:

    $ npm install moray

Or, to install the CLI tools and manual pages on your path:

    $ npm install -g moray

The CLI tools have detailed manual pages.  Start with `moray(1)` for an
overview.

The library interface has an overview manual page `moray(3)` that describes how
to initialize the client for Triton and Manta services and CLI tools.  The API
documentation below includes examples of using both the CLI tools and Node
client library interfaces.  It's worth reviewing `moray(1)` and `moray(3)` to
understand the basic conventions used below.


# Common Options

There are some options that can be used with several RPCs. They are documented
here, and linked to from each of the RPCs where they are valid to use.

##### <a name="option-etag">etag</a>

An `etag` that an already existing object must first have before being affected.
If the `etag` on the existing object differs from the one specified in the
options, then the server will return an `EtagConflictError`. See
[PutObject](#putobject) for further details.

##### <a name="option-limit">limit</a>

A number that limits the number of records that are processed by the request.
Default is `1000`, unless `noLimit` is set to `true`.

Using large values (> 1000, or setting `noLimit` to `true`) is considered
dangerous. The design center of Moray is around short-lived requests, and there
are serious consequences in PostgreSQL for leaving connections open in
transactions for extended periods while other transactions are making changes.

##### <a name="option-nobucketcache">noBucketCache</a>

A boolean which, when set to `true`, makes Moray refresh its bucket cache before
handling the request. Default is `false`.

The bucket cache should generally not be be disabled (i.e `noBucketCache` set to
`true`). It is key to ensuring that the performance of a Moray server scales
with the number of requests. This feature primarily exists to write internal
Moray tests.

##### <a name="option-no_count">no_count</a>

A boolean which, when set to `true`, makes Moray not include a `_count` property
on each record sent with the response. Default is `false`.

The purpose of this option is to avoid performing a potentially expensive
`COUNT` query in addition to the query that fetches data records.

##### <a name="option-nolimit">noLimit</a>

A boolean which, when set to `true` and `limit` is not set, makes Moray not have
a maximum number of records processed by the request. Default is `false`.

Using this option is __considered dangerous__. The design center of Moray is
around short-lived requests, and there are serious consequences in PostgreSQL
for leaving connections open in transactions for extended periods while other
transactions are making changes.

##### <a name="option-offset">offset</a>

A number which indicates the offset at which Moray starts processing records
that would be returned by the same request with no `offset`. Default is `0`.

##### <a name="option-requireonlinereindexing">requireOnlineReindexing</a>

When passing `requireOnlineReindexing: true`, the client will make sure that
the server supports safely reading the contents of a reindexing bucket. Recent
versions of the server will always behave correctly here. This option is to
allow clients to make certain that they receive an `UnhandledOptionsError`
instead of having their data corrupted if they suspect that they are talking
to an older server.

##### <a name="option-req_id">req_id</a>

A string that can be used to track a request. Default is an automatically
generated V4 UUID.

##### <a name="option-sort">sort</a>

An object or an array of objects with the following properties:

* `attribute`: a required property of type string representing a field name on
  which to sort the result

* `order`: an optional property of type string that is either `ASC` or `DESC` to
  sort with ascending or descending order respectively. Default value is `ASC`.

Default value is `undefined`.

##### <a name="option-timeout">timeout</a>

A number that represents the delay in milliseconds that the request waits on a
reply from its underlying Postgres query before it errors with a
`QueryTimeoutError`. Default is `30000` (30 seconds).

Using this option is __strongly discouraged__. For identifying network failures,
TCP keep-alive, which is what the node-moray client library uses, is a better
fit. Otherwise, if the request hasn't completed, then the database is stuck
processing that request, and applications cannot generally do anything safely
except to wait (or possibly raise an alarm). Moreover, expiration of this
timeout does not cancel the underlying PostgreSQL query and the corresponding
PostgreSQL resources remain in use until the query ultimately does complete.


# Buckets

## CreateBucket

Creates a new bucket given a name and a `config` object.  The `config` object
defines an `index` section and optionally a `pre`, `post` array of trigger
functions.  Also, note that buckets can be versioned, such that subsequent
updates will fail if the version rolls backwards.

### API

A "fully loaded" config (without post triggers) would look like this:

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
| options  | object   | any optional parameters, see below              |
| callback | function | only argument is `err`                          |

#### Options

- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `InvalidBucketConfigError`
* `InvalidBucketNameError`
* `InvalidIndexDefinitionError`
* `NoDatabasePeersError`
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
| options  | object   | any optional parameters, see below  |
| callback | function | only argument is `err`              |

#### Options

- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `NoDatabasePeersError`

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

| Field    | Type     | Description                        |
| -------- | -------- | ---------------------------------- |
| options  | object   | any optional parameters, see below |
| callback | function | only argument is `err`             |

#### Options

- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `NoDatabasePeersError`

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
for `reindexObjects` to work.)

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
| options  | object   | any optional parameters, see below              |
| callback | function | only argument is `err`                          |

#### Options

- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `InvalidBucketConfigError`
* `InvalidIndexDefinitionError`
* `NoDatabasePeersError`
* `NotFunctionError`

### CLI

    $ putbucket -s foo < test.json
    $ putbucket -u email -u userid:number foo

## DeleteBucket

Deletes a bucket, *and all data in that bucket!*

### API

    client.delBucket('foo', function (err) {
        assert.ifError(err);
    });

### Inputs

| Field    | Type     | Description                         |
| -------- | -------- | ----------------------------------- |
| name     | string   | globally unique name for the bucket |
| options  | object   | any optional parameters, see below  |
| callback | function | only argument is `err`              |

#### Options

- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `NoDatabasePeersError`

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

If the update has been successfully applied, then the new `etag` will be
returned to the client.

### API

    var data = {
        email: 'mark.cavage@joyent.com',
        ismanager: false
    };
    client.putObject('foo', 'mcavage', data, function (err, res) {
        assert.ifError(err);
        console.log(res.etag);
    });

### Inputs

| Field    | Type     | Description                             |
| -------- | -------- | --------------------------------------- |
| bucket   | string   | bucket to write this key in             |
| key      | string   | bucket to write this key in             |
| value    | object   | free-form JS object                     |
| options  | object   | any optional parameters, see below      |
| callback | function | only argument is `err`                  |

#### Options

- [etag](#option-etag)
- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `EtagConflictError`
* `NoDatabasePeersError`
* `UniqueAttributeError`
* `InvalidIndexTypeError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

    $ putobject -d '{"email": "mark.cavage@joyent.com"}' foo mcavage

## GetObject

Retrieves an object by bucket and key.

The `_txn_snap` field represents the postgres internal transaction *snapshot*
that corresponds to when this record was written (specifically the `xmin`
component).  To really understand this, you probably want to go read up on how
[MVCC is implemented in
postgres](http://momjian.us/main/writings/pgsql/internalpics.pdf); specifically
pp56-58.  The short of it is, for records in Moray, it is possible that more
than one record will have the same `_txn_snap` value, so you cannot rely on it
as a unique id.  You also cannot rely on this value to increase over time.
Objects with later `_txn_snap` values can be visible before objects with earlier
`_txn_snap` values.

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
| options  | object   | any optional parameters, see below         |
| callback | function | arguments of `err` and `obj`               |

#### Options

- [noBucketCache](#option-nobucketcache)
- [requireOnlineReindexing](#option-requireonlinereindexing)
- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `ObjectNotFoundError`
* `NoDatabasePeersError`

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
Note this is not a streaming API.  (Pagination must be implemented by consumers,
if desired.)

Search filters are fully specified according to search filters resembling LDAP
filter rules.  All fields included in your search query should be indexed in the
bucket config, though the server only enforces that at least one field that's
used to limit the result set is indexed (to avoid an obvious case where a table
scan is required).  **Surprising behavior results when searching with
non-indexed fields, and this is strongly discouraged.**  For details, see
`findobjects(1)`.

In addition to the search filter, you can specify `limit`, `offset`, and `sort`;
the first two act like they usually do in DBs, and `sort` must be a JSON object
specifying the attribute to sort by, and an order, which is one of `ASC` or
`DESC` (so also like DBs).  The default `limit` setting is `1000`. There is no
default `offset`.  The default `sort` order is `ASC`, and the default attribute
is `_id` (which means records are returned in the order they were created or
updated).

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
| options | object | any optional parameters, see below                     |

#### Options

- [limit](#option-limit)
- [noBucketCache](#option-nobucketcache)
- [noLimit](#option-nolimit)
- [offset](#option-offset)
- [requireOnlineReindexing](#option-requireonlinereindexing)
- [req_id](#option-req_id)
- [timeout](#option-timeout)

##### <a name="option-requireIndexes">requireIndexes</a>

When passing `requireIndexes: true`, `findObjects` requests will respond with a
`NotIndexedError` error if at least one of the fields included in the search
filter has an index that can't be used.

##### sql_only

A boolean which, if `true`, makes Moray send the SQL statement that would be
executed in the response instead of sending actual data records. Default is
`false`.

### Errors

* `BucketNotFoundError`
* `InvalidQueryError`
* `NoDatabasePeersError`
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
      "_count": 3,
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
      "_count": 3,
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
      "_count": 3,
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
| options  | object   | any optional parameters, see below      |
| callback | function | only argument is `err`                  |

#### Options

- [etag](#option-etag)
- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `EtagConflictError`
* `ObjectNotFoundError`
* `NoDatabasePeersError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

### CLI

    $ delobject foo mcavage


## UpdateObjects

Allows you to bulk update a set of objects in one transaction.  Note you can
only update indexed fields.  You call this API with a bucket, a list of fields
to update and a filter, that is exactly the same syntax as `findObjects`.

A few caveats:

- All objects affected by the update will have the same `_etag`.
- The `JSON value` will *not* be updated in situ, though Moray hides this fact
  from you.  Subsequent "get" operations will merge the results into the
  returned value.
- If an object has been stored with an array value in a field that has a scalar
  index type, attempts to use UpdateObjects on that field in that object will
  have no effect.

  This is done to maintain compatibility with how UFDS uses Moray. UFDS stores
  arrays of strings in fields with a `string` index. Moray then flattens the
  array into a comma-separated list of values, which gets stored in Postgres.
  Since these values cannot be reliably split back into the original array,
  Moray ignores the values in the Postgres column in favor of the values in the
  original JSON object.

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
| fields  | object | keys and values to update                              |
| filter  | string | search filter string                                   |
| options | object | any optional parameters, see below                     |

#### Options

- [limit](#option-limit)
- [noBucketCache](#option-nobucketcache)
- [noLimit](#option-nolimit)
- [offset](#option-offset)
- [req_id](#option-req_id)
- [sort](#option-sort)
- [timeout](#option-timeout)

## ReindexObjects

After performing an `updateBucket` which added index fields, `reindexObjects`
can be used to automatically re-write objects so that all index fields are
properly populated.  This operation does _not_ update the `mtime` or `etag`
fields on stale objects when they are re-indexed.  Since it requires a `count`
parameter to set the max rows per iteration, `reindexObjects` must be called
repeatedly until it reports 0 rows processed.  Only then will the added indexes
be made available for use.

The selected `count` value should be chosen with the expected bucket object size
in mind.  Too large a value can cause transactions to remain open for extended
periods, and may also cause Moray to consume excessive amounts of memory since
it's unable to exert backpressure on PostgreSQL when fetching rows.

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
| options | object | any optional parameters, see below           |

#### Options

- [no_count](#option-no_count)
- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `NoDatabasePeersError`

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
| options | object | any optional parameters, see below                     |

#### Options

- [limit](#option-limit)
- [noBucketCache](#option-nobucketcache)
- [noLimit](#option-nolimit)
- [offset](#option-offset)
- [req_id](#option-req_id)
- [sort](#option-sort)
- [timeout](#option-timeout)

## Batch

Allows you to write a list of operations to perform inside a single transaction,
so that all or none succeed. The list of operations is an Array of objects
containing an `"operation"` field set to:

- `"put"` (corresponds to [PutObject](#putobject)), to create or overwrite
  `"key"` with the specified `"value"` in `"bucket"`.
- `"delete"` (corresponds to [DeleteObject](#deleteobject)), to delete
  `"key"` from `"bucket"`.
- `"update"` (corresponds to [UpdateObjects](#updateobjects)), to update
  all objects in `"bucket"` that match the given `"filter"` with the values
  specified in `"fields"`.
- `"deleteMany"` (corresponds to [DeleteMany](#deletemany)), to
  delete all objects in `"bucket"` that match the given `"filter"`.

All of these objects can have an `"options"` field passed in which will be
interpreted in the same way they would be with the corresponding operation.
This is useful when performing `"put"` or `"delete"` operations, since you
can then specify an [etag](#option-etag) to check first, to avoid conflicts
with concurrent RPCs.

If no `"operation"` is specified, then the field will default to `"put"`.

When Moray executes a batch operation, it will perform the operations in the
specified order. This has some important implications: because updating a row
grabs a lock on it, it is possible for two transactions to create a deadlock.
For example, consider executing the two following updates in Postgres
simultaneously:

```
var batch1 = [
    {
        bucket: 'foo',
        key: 'a',
        value: { n: 1 },
        options: { etag: 'D903D93A' }
    },
    {
        bucket: 'foo',
        key: 'b',
        value: { n: 1 },
        options: { etag: 'C67C2A5A' }
    }
];
var batch2 = [
    {
        bucket: 'foo',
        key: 'b',
        value: { n: 1 },
        options: { etag: 'C67C2A5A' }
    },
    {
        bucket: 'foo',
        key: 'a',
        value: { n: 1 },
        options: { etag: 'D903D93A' }
    }
];
```

When this happens, Postgres will return a `"deadlock detected"` error. To
prevent this from happening, make sure to place your updates in a consistent
order, so that given bucket/key pairs are always updated and deleted in the
same order.

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
        bucket: 'jobs',
        operation: 'deleteMany'
        filter: '(state=finished)'
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
| options  | object   | any optional parameters, see below                                |
| callback | function | only argument is `err` and `meta` which will have a list of etags |

#### Options

- [etag](#option-etag)
- [req_id](#option-req_id)
- [timeout](#option-timeout)

### Errors

* `BucketNotFoundError`
* `EtagConflictError`
* `NoDatabasePeersError`
* `UniqueAttributeError`

Plus any currently unhandled Postgres Errors (such as relation already exists).

# Other

## sql

An API that really only exists for two reasons:

1. for humans to quickly get some debug info from Moray
2. for systems like UFDS that need to create extra tables et al in Moray for
   `post` triggers.

The latter use-case (filling in gaps in the Moray API) is considered deprecated.

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
| options   | object | any optional parameters, see below   |

#### Options

- [req_id](#option-req_id)
- [timeout](#option-timeout)

### CLI

    $ sql "select now() from buckets_config;"
    {
        "now": "2012-08-01T15:50:33.291Z"
    }


# Additional features

## <a name="using-arrays">Using Arrays</a>

Here's a small example demonstrating how to use arrays:

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
and on writes, Moray knows how to index those properly.  On searches, the
contract is to return the record if _any_ of the array values match the filter
subclause.  There is one caveat:  _wildcard_ searches do not work (and can't).
So doing `(name=f*)` will return an error to you.  The reason is that Postgres
does not have any sane way of doing this (it is technically possible, but
expensive, and not currently implemented in Moray).

## <a name="using-ranges">Using Ranges</a>

Moray has two types, `numrange` and `daterange`, which use Postgres's
[range types](https://www.postgresql.org/docs/9.2/rangetypes.html). Using these
types, you can describe ranges of numbers and dates in the following syntax:

- `[a,b]`, an inclusive range from `a` to `b`
- `(a,b)`, an exclusive range from `a` to `b`
- `[a,b)`, a range from `a` (inclusive) to `b` (exclusive)
- `(a,b]`, a range from `a` (exclusive) to `b` (inclusive)

The left side of the range may be omitted to have no lower bound, and the right
side may be omitted to have no upper bound. Omitting both sides (e.g., `[,]`)
creates a range that contains all values.

When using ranges, the extensible filters `overlaps`, `contains`, and `within`
may be used to search for values within ranges. These filters may also be used
with the `subnet` type, where values within a subnet are of type `ip`.

### Example

    $ putbucket -i numr:numrange -i num:number ranges
    $ putobject -d '{"numr":"[1,4]","num":5}' ranges obj1
    $ putobject -d '{"numr":"[3,10]","num":7}' ranges obj2
    $ putobject -d '{"numr":"[8,15]","num":-6}' ranges obj3
    $ findobjects ranges '(numr:overlaps:=\(3,7\))'
    {
      "bucket": "ranges",
      "key": "obj1",
      "value": {
        "numr": "[1,4]",
        "num": 5
      },
      "_id": 1,
      "_etag": "2A7CFC8E",
      "_mtime": 1562762031876,
      "_txn_snap": null,
      "_count": 2
    }
    {
      "bucket": "ranges",
      "key": "obj2",
      "value": {
        "numr": "[3,10]",
        "num": 7
      },
      "_id": 2,
      "_etag": "F4B3DD9D",
      "_mtime": 1562762057352,
      "_txn_snap": null,
      "_count": 2
    }
    $ findobjects ranges '(numr:contains:=4)'
    {
      "bucket": "ranges",
      "key": "obj1",
      "value": {
        "numr": "[1,4]",
        "num": 5
      },
      "_id": 1,
      "_etag": "2A7CFC8E",
      "_mtime": 1562762031876,
      "_txn_snap": null,
      "_count": 2
    }
    {
      "bucket": "ranges",
      "key": "obj2",
      "value": {
        "numr": "[3,10]",
        "num": 7
      },
      "_id": 2,
      "_etag": "F4B3DD9D",
      "_mtime": 1562762057352,
      "_txn_snap": null,
      "_count": 2
    }
    $ findobjects ranges '(num:within:=[,0])'
    {
      "bucket": "ranges",
      "key": "obj3",
      "value": {
        "numr": "[8,15]",
        "num": -6
      },
      "_id": 3,
      "_etag": "373496EE",
      "_mtime": 1562762059101,
      "_txn_snap": null,
      "_count": 1
    }

## Triggers

While a quasi-advanced topic, Moray does support `pre` and `post` "triggers" on
buckets, which are guaranteed to run before and after a write
(so put|del object) is performed.  You write them as just plain old JS functions
when you create your bucket, and note that they are guaranteed to run as part of
the transaction scope.  You typically will use `pre` triggers to transform
and/or validate data before being saved, and use `post` triggers to write an
audit log or some such thing (UFDS leverages `post` triggers to generate the
changelog).  They are guaranteed to run in the order you define them.  Note that
`pre` triggers only run before `putObject`, whereas `post` runs after both
`putObject` and `delObject`.

In general, you need to be super careful with these, as they run in the same VM
that Moray does, so any uncaught exception will bring down the whole server.
This is intentional for performance reasons.  Additionally, they run in the same
callback chain as the rest of the operation, so failure to invoke the callback
will stall the operation and leak the Postgres handle.  So basically, don't
screw up.

### pre

The definition for a `pre` trigger looks like this:

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

The definition for a `post` trigger is nearly identical:


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
