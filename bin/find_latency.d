#!/usr/sbin/dtrace -s
#pragma D option quiet

moray*:::findobjects-start
{
	latency[arg0] = timestamp;
	bucket[arg0] = copyinstr(arg3);
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
