// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// This file exists to run "before all tests" things. So namely, we go ahead
// and create a scratch database here to run unit tests in.
//
// Postgres 9.1 comamnds must be in your $PATH
//

require('shelljs/global');


///--- Globals

var DBHOME = '/tmp/moray_unit_test' || process.env.MORAY_DB_HOME;



///--- Set up scratch area for database

function error(msg) {
    echo(msg);
    exit(1);
}

mkdir('-p', DBHOME);

if (exec('pg_ctl init -w -D ' + DBHOME).code !== 0)
    error('Error: pg_ctl init failed');

if (exec('pg_ctl start -w -D ' + DBHOME).code !== 0)
    error('Error: pg_ctl start failed');

exec('createdb', process.env.USER); // this might already exist
if (exec('createdb', 'test').code !== 0)
    error('Unable to create database');

if (exec('psql -f ' +__dirname + '/createuser.sql test').code !== 0)
    error('Unable to create user');
