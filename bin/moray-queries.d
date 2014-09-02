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

moray*:::query-start
{
	starts[copyinstr(arg0)] = timestamp;
	this->t = walltimestamp % 1000000000;

	printf("[%Y.%09d] Query %5s  (%d) > %s\n",
	       walltimestamp, this->t, copyinstr(arg0), timestamp,
	       copyinstr(arg1));

	this->t = 0;
}


moray*:::query-done
/starts[copyinstr(arg0)]/
{
	this->l = (timestamp - starts[copyinstr(arg0)]) / 1000;
	this->t = walltimestamp % 1000000000;

        printf("[%Y.%09d] Query %5s  (%d) done < (%dus)\n",
	       walltimestamp, this->t, copyinstr(arg0), timestamp, this->l);

	starts[copyinstr(arg0)] = 0;
	this->l = 0;
	this->q = 0;
	this->t = 0;
}
