#!/usr/sbin/dtrace -Cs
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

#pragma D option quiet

moray*:::pool-checkout
{
        @nconn[pid] = max(arg0 - arg1 - arg2);
        @qlen[pid] = max(arg3);
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

moray*:::reindex*-start
{
        @reindex[pid] = count();
}

BEGIN
{
        lines = 0;
}

profile:::tick-1sec
/lines < 1/
{
        /* print the header every 5 seconds */
        lines = 5;
        printf("         ------PG------  --------------MORAY---------------\n");
        printf("PID      CONN QLEN  OPS  GETS FIND PUTS UPDS DELS BTCH RIDX\n");
}
profile:::tick-1sec
{
        lines -= 1;
        printa("%-8d %@4u %@4u %@4u  %@4u %@4u %@4u %@4u %@4u %@4d    0\n",
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
