#!/usr/sbin/dtrace -s
#pragma D option quiet

/* This script must be run on the Postgres Master */

postgresql*:::query-start
{
	self->start = timestamp;
}


postgresql*:::query-done
/self->start/
{
	this->l = (timestamp - self->start) / 100000;
	this->q = copyinstr(arg0);
	this->t = walltimestamp % 1000000000;

        printf("[%Y.%09d] Query (%dus) > %s\n",
	       walltimestamp, this->t, this->l, this->q);

	self->start = 0;
	this->l = 0;
	this->q = 0;
	this->t = 0;
}
