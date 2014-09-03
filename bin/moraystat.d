#!/usr/sbin/dtrace -Cs
/* Run with -DMORAY_LEGACY on older moray systems */
#ifdef MORAY_LEGACY
#define PGPROBE pgpool-*
#else
#define PGPROBE moray-pgpool-*
#endif

#pragma D option quiet

PGPROBE:::acquire
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

#ifndef MORAY_LEGACY
/* not supported in older versions of moray */
moray*:::reindex*-start
{
        @reindex[pid] = count();
}
#endif

BEGIN
{
        printf("         ------PG------  --------------MORAY---------------\n");
        printf("PID      CONN QLEN  OPS  GETS FIND PUTS UPDS DELS BTCH RIDX\n");
}
profile:::tick-1sec
{
#ifndef MORAY_LEGACY
        printa("%-8d %@4u %@4u %@4u  %@4u %@4u %@4u %@4u %@4u %@4d %@4d\n",
                        @nconn, @qlen, @query,
                        @gets, @finds, @puts, @upds, @dels, @batch, @reindex
        );
        clear(@reindex);
#else
        printa("%-8d %@4u %@4u %@4u  %@4u %@4u %@4u %@4u %@4u %@4d    0\n",
                        @nconn, @qlen, @query,
                        @gets, @finds, @puts, @upds, @dels, @batch
        );
#endif
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
