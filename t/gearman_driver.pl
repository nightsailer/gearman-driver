#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;
use Live::NS3::AddJob;

my $driver = TestLib->gearman_driver;

my $w1 = Live::NS3::AddJob->new();

$driver->add_job(
    {
        decode     => 'custom_decode',
        encode     => 'custom_encode',
        max_childs => 5,
        method     => $w1->meta->find_method_by_name('job1')->body,
        min_childs => 1,
        name       => 'Live::NS3::AddJob::job1',
        object     => $w1,
    }
);

$driver->add_job(
    {
        max_childs => 1,
        method     => $w1->meta->find_method_by_name('job2')->body,
        min_childs => 1,
        name       => 'Live::NS3::AddJob::job2',
        object     => $w1,
    }
);

$driver->run;
