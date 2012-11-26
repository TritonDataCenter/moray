#!/usr/sbin/dtrace -s
#pragma D option quiet

moray*:::putobject-start
{
	latency[arg0] = timestamp;
	bucket[arg0] = copyinstr(arg1);
	key[arg0] = copyinstr(arg2);
}

moray*:::putobject-done
/latency[arg0]/
{
	this->name = strjoin(strjoin(bucket[arg0], "/"), key[arg0]);
	@[this->name] = quantize(((timestamp - latency[arg0]) / 1000000));

	latency[arg0] = 0;
	bucket[arg0] = 0;
	key[arg0] = 0;
}
