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
#pragma D option strsize=8k

moray*:::putobject-start
/copyinstr(arg2) == $1/
{
        req[arg0] = timestamp;
        key[arg0] = copyinstr(arg3);
        val[arg0] = copyinstr(arg4);
}

moray*:::getobject-start
/copyinstr(arg2) == $1/
{
        req[arg0] = timestamp;
        key[arg0] = copyinstr(arg3);
}

moray*:::delobject-start
/copyinstr(arg2) == $1/
{
        req[arg0] = timestamp;
        key[arg0] = copyinstr(arg3);
}

moray*:::findobjects-start
/copyinstr(arg2) == $1/
{
        req[arg0] = timestamp;
        filter[arg0] = copyinstr(arg3);
}

fast*:::rpc-msg
/req[arg1]/
{
        code[arg1] = (arg2 == 3 ? "error" : "ok");
}

moray*:::putobject-done
/req[arg0]/
{
        printf("\n\nPUT\n");
        printf("\tkey  => %s\n", key[arg0]);
        printf("\tval  => %s\n", val[arg0]);
        printf("\tcode => %s\n", code[arg0]);
        printf("\ttime => %dms\n", ((timestamp - req[arg0]) / 1000000));
}


moray*:::getobject-done
/req[arg0]/
{
        printf("\n\nGET\n");
        printf("\tkey  => %s\n", key[arg0]);
        printf("\tval  => %s\n", copyinstr(arg1));
        printf("\tcode => %s\n", code[arg0]);
        printf("\ttime => %dms\n", ((timestamp - req[arg0]) / 1000000));
}

moray*:::delobject-done
/req[arg0]/
{
        printf("\n\nDELETE\n");
        printf("\tkey  => %s\n", key[arg0]);
        printf("\tcode => %s\n", code[arg0]);
        printf("\time  => %dms\n", ((timestamp - req[arg0]) / 1000000));
}

moray*:::findobjects-done
/req[arg0]/
{
        printf("\n\nFIND\n");
        printf("\tfilter  => %s\n", filter[arg0]);
        printf("\trecords => %d\n", arg1);
        printf("\tcode    => %s\n", code[arg0]);
        printf("\ttime    => %dms\n", ((timestamp - req[arg0]) / 1000000));
}

moray*:::*objec*-done
{
        req[arg0] = 0;
        code[arg0] = 0;
        key[arg0] = 0;
        val[arg0] = 0;
}
