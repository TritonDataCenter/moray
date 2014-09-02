#!/usr/sbin/dtrace -s
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

#pragma D option quiet

/* This script must be run on the Postgres Master */

postgresql*:::query-start
{
	self->start = timestamp;
	this->t = walltimestamp % 1000000000;

	printf("[%Y.%09d] Query (%d) > %s\n",
	       walltimestamp, this->t, timestamp, copyinstr(arg0));

	this->t = 0;
}


postgresql*:::query-done
/self->start/
{
	this->l = (timestamp - self->start) / 1000;
	this->t = walltimestamp % 1000000000;

        printf("[%Y.%09d] Query (%d) done < (%dus)\n",
	       walltimestamp, this->t, timestamp, this->l);

	self->start = 0;
	this->l = 0;
	this->q = 0;
	this->t = 0;
}
