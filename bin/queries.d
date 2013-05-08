#!/usr/sbin/dtrace -s
#pragma D option quiet

/* This script must be run on the Postgres Master */

postgresql*:::query-start
{
	self->start = timestamp;
	this->t = walltimestamp % 1000000000;

	printf("[%Y.%09d] Query (%d) > %s\n",
	       walltimestamp, this->t, timestamp, copyinstr(arg0));

	this->t = 0;
}


postgresql*:::query-done
/self->start/
{
	this->l = (timestamp - self->start) / 1000;
	this->t = walltimestamp % 1000000000;

        printf("[%Y.%09d] Query (%d) done < (%dus)\n",
	       walltimestamp, this->t, timestamp, this->l);

	self->start = 0;
	this->l = 0;
	this->q = 0;
	this->t = 0;
}
