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

    createdb -U postgres moray
    psql -U postgres moray

    moray=# CREATE TABLE buckets_config (
        name text PRIMARY KEY,
        index text NOT NULL,
        pre text NOT NULL,
        post text NOT NULL,
        mtime timestamp without time zone DEFAULT now() NOT NULL
    );

Note if you want to use a different database name than `moray`, you can, you
just need to set the environment variable `MORAY_DB_NAME` to whatever you want
before starting the server.

Once the above is done, edit `./etc/config.laptop.json` file to have the correct
ZooKeeper endpoint(s) and domain name (note the domain name is the DNS name of
the manatee to back this Moray instance - that DNS name is "mapped" into
ZooKeeper). Then, source in ./dev_env.sh (this ensures you have the moray node
et al) and run:

    . ./dev_env.sh
    moray

Which will open up Moray on port 2020.  You can now use the CLI in
`node-moray.git` or whatever other means you want of talking to the server.

# Testing

    make prepush
