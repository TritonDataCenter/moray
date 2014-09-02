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

moray*:::findobjects-start
{
	latency[arg0] = timestamp;
	bucket[arg0] = copyinstr(arg2);
	filter[arg0] = copyinstr(arg3);
}

moray*:::findobjects-done
/latency[arg0]/
{
	@[bucket[arg0], filter[arg0], arg1] =
		quantize(((timestamp - latency[arg0]) / 1000000));

	latency[arg0] = 0;
	bucket[arg0] = 0;
	filter[arg0] = 0;
}
