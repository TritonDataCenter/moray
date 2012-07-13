# Joyent Engineering Guide

Repository: <git@git.joyent.com:orca.git>
Browsing: <https://mo.joyent.com/orca>
Who: Mark Cavage, Yunong Xiao
Docs: <https://head.no.de/docs/orca>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This repo contains Moray, the highly-available key/value store cowboy'd up by
Joyent.


# Development

You need a Postgres instance up and running first, so do this:

    pkgin -y install postgresql91-client postgresql91-adminpack \
        postgresql91-server
    svcadm enable postgresql:pg91
    createdb -U postgres moray
    Password: postgres

OPTIONAL: After doing that, then go ahead and hack up
`/var/pgsql/data91/pg_hba.conf` and `s|password|trust|` in all the lines at
the bottom. After that you can stop being prompted for passwords.

Then get the Moray server:

git clone git@git.joyent.com:moray.git
    cd moray
    make

edit the ./etc/config.laptop.json file to have the Postgres URL as

    pg://postgres:@localhost/moray

Then, source in ./dev_env.sh (this ensures you have the moray node et al) and
run:

    . ./dev_env.sh
    node main.js -f ./etc/config.laptop.json 2>&1 | bunyan

Before commiting/pushing run `make prepush` and get a code review from either
Mark or Yunong.

# Testing

    LOG_LEVEL=$level make test

# Design

TODO :)


# TODO

Remaining work for this repo:

- everything...
