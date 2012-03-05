# Joyent Engineering Guide

Repository: <git@git.joyent.com:orca.git>
Browsing: <https://mo.joyent.com/orca>
Who: Mark Cavage, Yunong Xiao
Docs: <https://head.no.de/docs/orca>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This repo contains Orca, the highly-available key/value store cowboy'd up by
Joyent.


# Development

To run the Orca server:

    git clone git@git.joyent.com:orca.git
    cd orca
    git submodule update --init
    make all
    make start

To update the docs, edit "docs/index.restdown" and run `make docs`
to update "docs/index.html".

Before commiting/pushing run `make prepush` and get a code review from either
Mark or Yunong.



# Testing

You need a Postgres running:

    mkdir /var/db/orca
    pgctl -D /var/db/orca start
    createdb test

Then:

    make test

If you want log verbosity:

    LOG_LEVEL=debug make test

# Design

TODO :)


# TODO

Remaining work for this repo:

- everything...
