<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019 Joyent, Inc.
    Copyright 2024 MNX Cloud, Inc.
-->

# Moray, the highly-available key-value store

This repository is part of the Joyent Triton and Manta projects.
For contribution guidelines, issues, and general documentation, visit the main
[Triton](http://github.com/TritonDataCenter/triton) and
[Manta](http://github.com/TritonDataCenter/manta) project pages.

This repository contains Moray, the highly-available key-value store from
Joyent.  The Moray service provides a simple put/get/search/delete abstraction
on top of Postgres 9.x, over plain TCP using
[node-fast](https://github.com/TritonDataCenter/node-fast).


## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/TritonDataCenter/manta/blob/master/docs/mantav2.md)
for details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta. This is the version used by Triton.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.


## Introduction

For basic information about how to use Moray and what operations Moray supports,
see the moray(1) and related manual pages in the [Moray
client](https://github.com/TritonDataCenter/node-moray/) repository.

For reference documentation about the RPC calls and the node-moray library calls
used to invoke them, see the [developer reference](./docs/index.md)
documentation inside this repository.

The rest of this README describes how Moray is deployed and how to build, run,
and test Moray.


## Overview

Moray implements a JSON key-value interface to a PostgreSQL database.  It serves
several functions:

* Moray provides a reasonably simple key-value abstraction over PostgreSQL,
  serving a similar role as an ORM.  _Buckets_ are implemented with tables,
  _objects_ are implemented as rows, and operations are translated into SQL
  queries.  The abstractions provided are oriented around very large buckets
  and attempt to avoid exposing operations that would not scale accordingly.
* Moray provides pooling of PostgreSQL connections (similar to pgbouncer).
  Moray clients maintain persistent TCP connections that can be idle for
  extended periods.  The Moray server multiplexes incoming requests over some
  fixed number of PostgreSQL connections.
* In a database cluster deployed using
  [Manatee](https://github.com/TritonDataCenter/manatee), Moray is responsible
  for tracking the cluster state so that queries are always dispatched to the
  current PostgreSQL primary.

In Triton and Manta, PostgreSQL is typically deployed atop Manatee, which
provides high availability for PostgreSQL using synchronous replication and
automated failover.  Multiple Moray instances are typically deployed atop that.
Here's a diagram of the containers used in a typical Manatee/Moray _shard_:

             +-------+     +-------+     +-------+
             | moray |     | moray |     | moray | ...
             +-+---+-+     +-+---+-+     +-+---+-+
               |   |         |   |         |   +--------------------------+
               |   |         |   +-------- | ---------------------------+ |
               |   +-------- | ----------- | -------------------------+ | |
               |             |             |                          | | |
            PG |+------------+             |                          | | |
       queries ||+-------------------------+                    +-----+-+-+-+
               |||                                              |           |
               |||   +----------------------------------------- + ZooKeeper |
               |||   |             +--------------------------- + Cluster   |
               |||   |             |             +------------- +           |
            +--+++---+-+  +--------+-+  +--------+-+            +-----------+
            | manatee  |  | manatee  |  | manatee  |
            |----------+  |----------+  |----------+ ...
            | postgres |  | postgres |  | postgres |
            | primary  |  | sync     |  | async    |
            +----------+  +----------+  +----------+

This works as follows:

* Incoming Moray data requests (e.g., for reading and writing of key-value
  pairs) are translated into SQL queries against the underlying PostgreSQL
  database.
* The Manatee component supervises the underlying PostgreSQL database instances
  and ensures that only one of them is writable at any given time.  The
  non-writable instances are used to support rapid failover in the event of
  failure of the primary instance.  Manatee automatically records the current
  cluster state (including which peer is the primary) in ZooKeeper.
* Each Moray instance reads the cluster state from ZooKeeper to make sure that
  incoming requests are dispatched only to the current PostgreSQL primary.

All state is stored in ZooKeeper and PostgreSQL.  As a result, additional
Moray instances can be deployed for both horizontal scalability and fault
tolerance of Moray itself.


## Building and running Moray

Be sure to read the Introduction section above that describes how Moray is
deployed.  It's also assumed that you're familiar with setting up a Triton or
Manta development zone (container).

To work on Moray, you'll need Manatee and ZooKeeper clusters available.  You can
set these up on your own, but usually it's easier to configure your development
Moray instance to use the Manatee and ZooKeeper clusters in an existing Triton
or Manta deployment.  You can even use the same PostgreSQL database, or you can
create your own by executing this on the primary Manatee instance:

    # createdb -U postgres -O moray moray
    # psql -U postgres moray
    moray=# CREATE TABLE buckets_config (
        name text PRIMARY KEY,
        index text NOT NULL,
        pre text NOT NULL,
        post text NOT NULL,
        options text,
        mtime timestamp without time zone DEFAULT now() NOT NULL
    );

    moray=# alter table buckets_config owner to moray

Note if you want to use a different database name than `moray`, you can; you
just need to modify the above commands accordingly and then set the environment
variable `MORAY_DB_NAME` to the desired database name before starting the
server.

Once you've got a Manatee and ZooKeeper cluster available and created the
database, you'll need to create a Moray server configuration file.  Again, you
can create your own from scratch, but it's usually easier to copy the
configuration file from `/opt/smartdc/moray/etc/config.json` in a Moray zone
inside an existing Triton or Manta deployment.  If you want to create your own,
you can start with the template configuration file in ./sapi\_manifests, but the
configuration file properties are currently not documented.

Now, in your development zone, build this repository:

    $ make

Now source ./env.sh so that you've got a PATH that includes the right version of
Node:

    $ source ./env.sh

and run Moray using your configuration file:

    node main.js -f YOUR_CONFIG_FILE -v 2>&1 | bunyan

By default, Moray listens on port 2020.  You can use the CLI tools in
[node-moray](https://github.com/TritonDataCenter/node-moray) to start working
with the server.  Those tools have detailed manual pages.


## Testing

For testing, see the separate
[moray-test-suite](https://github.com/TritonDataCenter/moray-test-suite)
repository.  You will need to build and run a Moray instance as described above,
and then follow the instructions in that repository to test it.

You should consider pointing your testing instance at a different DB than
`moray` to avoid interfering with operations in your Triton or Manta deployment.
There is an unsupported script in `tools/coal-test-env.sh` that will create a
`moray_test` DB for you and run an additional `moray-test` instance listening at
port `2222`.  Just scp'ing it into the global zone and executing it should work.
You will need to configure the test suite appropriately (see the README.md in
the moray-test-suite repository).

## Running in standalone mode

Moray can be used as a library to run a standalone Moray server that talks to a
Postgres database without using Manatee. A single function is exported,
`createServer`, which takes an object with the following fields:

- `log`, a [bunyan](https://github.com/trentm/node-bunyan) logger
- `port`, the TCP port to listen on
- `bindip`, the IP address to bind to
- `audit`, a boolean indicating whether to log the result and duration of all
  requests
- `kangPort`, the port the Kang server should listen on
- `collector`, an [artedi](https://github.com/TritonDataCenter/node-artedi)
  metric collector
- `standalone`, an object specifying the standalone server's configuration:
    * `pg`, an object which specifies the Postgres client pool confguration:
        - `queryTimeout`, how long (in milliseconds) before a query is timed out
          (defaults to 0, which disables the timeout)
        - `maxConnections`, the maximum number of connections to maintain
          to Postgres
    * `url`, a [pg](https://github.com/brianc/node-postgres) URL describing how
      to connect to the server (i.e., `/path/to/unix/socket/dir dbName`)

`createServer` returns a server object with a `listen()` method to start the
server. The server will emit a `ready` event once it's started up.

## Monitoring

Moray exposes metric and Kang internal state information accessible through a
REST API. The port used for the monitoring server is provided as the `-k`
argument when starting the Moray server.

### Kang

[Kang](https://github.com/davepacheco/kang) data can be retrieved from Moray
by issuing `GET /kang/snapshot` on the Kang port.

For example, a `curl` command assuming the monitoring server is running on
port 3020 of the local host:

```
$ curl http://localhost:3020/kang/snapshot
```

Kang also offers a command-line interactive debugger, which ships in Moray
zones. This can be used to combine Kang data from multiple Moray processes.

For example, if we have four Moray processes inside a Moray zone we can use this
command to view their state simultaneously:

```
$ kang -h localhost:3021,localhost:3022,localhost:3023,localhost:3024

```

### Metrics

Application metrics can be retrieved from the route `GET /metrics`, also on the
Kang port. The metrics are returned in the [Prometheus](https://prometheus.io/)
v0.0.4 text format.

For example, a `curl` command can be used to scrape metrics:

```
$ curl http://localhost:3020/metrics
```

The following metrics are always collected:

- Open Postgres connections
- Available Postgres connections
- Pending Postgres connections
- Backend request queue length
- End-to-end latency for all requests
- Count of requests completed
- Count of of all Cueball failure events

Each of the metrics returned include the following metadata labels:

- Datacenter name (i.e. us-east-1)
- CN UUID
- Zone UUID
- PID

The request latency and request counter metrics additionally include a metadata
label denoting the RPC method used (e.g. 'getbucket').

The metric collection functionality is intended to be consumed by a monitoring
service like a Prometheus or InfluxDB server.

## License

This Source Code Form is subject to the terms of the Mozilla Public License, v.
2.0.  For the full license text see LICENSE, or http://mozilla.org/MPL/2.0/.

Copyright (c) 2017, Joyent, Inc.
