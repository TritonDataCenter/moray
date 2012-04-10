#!/usr/sbin/dtrace -Z -s

#pragma D option quiet

BEGIN
{
    requests = 0;
}

moray*:::putobject-start
{
    track[arg0] = timestamp;
}

moray*:::putobject-done
/track[arg0]/
{
    @puts = lquantize(((timestamp - track[arg0]) / 1000000), 0, 512, 50);
    track[arg0] = 0;
    requests++;
}


profile:::tick-1sec
/requests > 0/
{
    printf("\nRequests per second: %d", requests);
    printa(@puts);
    requests = 0;
}
