<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2017, Joyent, Inc.
-->

# Moray, the highly-available key/value store

This repo contains Moray, the highly-available key/value store from Joyent.
Moray offers a simple put/get/del/search protocol on top of Postgres 9.x,
using the [node-fast](https://github.com/joyent/node-fast) RPC protocol.

This repository is part of the Joyent Manta and Triton projects. For
contribution guidelines, issues, and general documentation, visit the main
[Triton](http://github.com/joyent/triton) and
[Manta](http://github.com/joyent/manta) project pages.

## Development

You'll want a [Manatee](https://github.com/joyent/manatee) instance up and
running first (which itself requires ZooKeeper),  so the easiest way is to
point at an existing COAL or Manta standup.  Once you have that, you'll need
to create a database and the minimal schema necessary to bootstrap moray
(issue these against whatever DB is currently `primary` in manatee):

    createdb -U postgres -O moray moray
    psql -U postgres moray

    moray=# CREATE TABLE buckets_config (
        name text PRIMARY KEY,
        index text NOT NULL,
        pre text NOT NULL,
        post text NOT NULL,
        options text,
        mtime timestamp without time zone DEFAULT now() NOT NULL
    );

    moray=# alter table buckets_config owner to moray

Note if you want to use a different database name than `moray`, you can, you
just need to set the environment variable `MORAY_DB_NAME` to whatever you want
before starting the server.

Once the above is done, edit one of the JSON files in `./etc/` (based on whether
you're using an SDC or Manatee, and whether COAL or lab) to have the correct
ZooKeeper endpoint(s) and domain name (note the domain name is the DNS name of
the manatee to back this Moray instance - that DNS name is "mapped" into
ZooKeeper). If in doubt, compare against the configuration for the Moray zone
deployed atop the Manatee you're deploying against. Then, source in ./env.sh
(this ensures you have the moray node et al) and run:

    . ./env.sh
    server

Which will open up Moray on port 2020.  You can now use the CLI in
`node-moray.git` or whatever other means you want of talking to the server.

## Testing

For testing, see the separate
[moray-test-suite](https://github.com/joyent/moray-test-suite) repository.  You
will need to supply your own server configuration file.  You should consider
pointing the config file to a different DB than `moray`. There is a script at
`tools/coal-test-env.sh` which will create a `moray_test` DB for you and run an
additional `moray-test` instance listening at port `2222`. Just scping into GZ
and executing it should work.  You will need to configure the test suite
appropriately (see the README.md in the moray-test-suite repository).

## Running in standalone mode

Moray can be used as a library to run a standalone Moray server that talks to a
Postgres database without using Manatee. A single function is exported,
`createServer`, which takes an object with the following fields:

- `log`, a [bunyan](https://github.com/trentm/node-bunyan) logger
- `port`, the TCP port to listen on
- `bindip`, the IP address to bind to
- `audit`, a boolean indicating whether to log the result and duration of all
  requests
- `standalone`, an object specifying the standalone server's configuration:
    * `pg`, an object which specifies the Postgres client pool confguration:
        - `queryTimeout`, how long (in milliseconds) before a query is timed out
        - `maxConnections`, the maximum number of connections to maintain
          to Postgres
    * `url`, a [pg](https://github.com/brianc/node-postgres) URL describing how
      to connect to the server (i.e., `/path/to/unix/socket/dir dbName`)

`createServer` returns a server object with a `listen()` method to start the
server. The server will emit a `ready` event once it's started up.

## License

This Source Code Form is subject to the terms of the Mozilla Public License, v.
2.0.  For the full license text see LICENSE, or http://mozilla.org/MPL/2.0/.

Copyright (c) 2016, Joyent, Inc.
