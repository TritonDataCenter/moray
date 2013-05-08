#!/usr/sbin/dtrace -s
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
