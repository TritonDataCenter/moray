#!/usr/sbin/dtrace -s

#
# findslow.d MILLISECONDS: report "findobjects" queries taking longer than
# MILLISECONDS milliseconds to complete.
#

#pragma D option quiet

moray*:::findobjects-start
{
        latency[arg0] = timestamp;
        bucket[arg0] = copyinstr(arg2);
        filter[arg0] = copyinstr(arg3);
}

moray*:::findobjects-done
/latency[arg0] && timestamp - latency[arg0] > $1 * 1000000/
{
        printf("%5dms %-20s %3d %s\n",
            ((timestamp - latency[arg0]) / 1000000),
            bucket[arg0], arg1, filter[arg0]);
}

moray*:::findobjects-done
{
        latency[arg0] = 0;
        bucket[arg0] = 0;
        filter[arg0] = 0;
}
