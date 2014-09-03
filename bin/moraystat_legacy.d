#!/usr/sbin/dtrace -s
#pragma D option quiet

/* moraystat_legacy.d is designed to work on older version of moray. */
/* It uses the older pgpool-* probe name and lacks reindex counters */

pgpool-*:::acquire
{
        this->j = copyinstr(arg1);
        @nconn[pid] = max(strtoll(json(this->j, "resources")) - strtoll(json(this->j, "available")));
        @qlen[pid] = max(strtoll(json(this->j, "queue")));
}
moray*:::query-start
{
        @query[pid] = count();
}

moray*:::getobject-start
{
        @gets[pid] = count();
}
moray*:::findobjects-start
{
        @finds[pid] = count();
}
moray*:::putobject-start
{
        @puts[pid] = count();
}
moray*:::update-start
{
        @upds[pid] = count();
}
moray*:::delmany-start,
moray*:::delobject-start
{
        @dels[pid] = count();
}
moray*:::batch-start
{
        @batch[pid] = count();
}

BEGIN
{
        printf("         ------PG------  ------------MORAY------------\n");
        printf("PID      CONN QLEN  OPS  GETS FIND PUTS UPDS DELS BTCH\n");
}
profile:::tick-1sec
{
        printa("%-8d %@4u %@4u %@4u  %@4u %@4u %@4u %@4u %@4u %@4d\n",
                        @nconn, @qlen, @query,
                        @gets, @finds, @puts, @upds, @dels, @batch
        );
        clear(@nconn);
        clear(@qlen);
        clear(@query);
        clear(@gets);
        clear(@finds);
        clear(@puts);
        clear(@upds);
        clear(@dels);
        clear(@batch);
}
