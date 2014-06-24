# Moray, the highly-available key/value store

Repository: <git@git.joyent.com:moray.git>
Browsing: <https://mo.joyent.com/moray>
Who: Mark Cavage
Docs: <https://mo.joyent.com/docs/moray>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This repo contains Moray, the highly-available key/value store from Joyent.
Moray offers a simple put/get/del (as well as search) protocol on top of
Postgres 9.x, over plain TCP see <https://github.com/mcavage/node-fast>.

# Development

You'll want a Manatee instance up and running first (which itself requires
ZooKeeper),  so the easiest way is to point at an existing COAL or Manta
standup.  Once you have that, you'll need to create a database and the minimal
schema necessary to bootstrap moray (issue these against whatever DB is
currently `primary` in manatee):

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

# Testing

Before run tests, you should consider point config file to a different DB
than `moray`. There is a script at `tools/coal-test-env.sh` which will create
a `moray_test` DB for you and run an additional `moray-test` instance listening
at port `2222`. Just scping into GZ and executing it should work.

Then, make sure your test file points to the right port:

    MORAY_PORT=2222 make test

To run tests on default `2020` port just do:

    make test
