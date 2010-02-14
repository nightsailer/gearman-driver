#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver::Test;
use Gearman::Driver::Test::Live::NS3::AddJob;

my $driver = Gearman::Driver::Test->gearman_driver;

my $w1 = Gearman::Driver::Test::Live::NS3::AddJob->new();

$driver->add_job(
    {
        max_processes => 5,
        min_processes => 1,
        name          => 'job_group_1',
        worker        => $w1,
        methods       => [
            {
                body   => $w1->meta->find_method_by_name('job1')->body,
                decode => 'custom_decode',
                encode => 'custom_encode',
                name   => 'job1',
            },
            {
                body => $w1->meta->find_method_by_name('begin_end')->body,
                name => 'begin_end',
            }
        ]
    }
);

$driver->add_job(
    {
        max_processes => 4,
        min_processes => 4,
        name          => 'four_processes',
        worker        => $w1,
        methods       => [
            {
                body => $w1->meta->find_method_by_name('four_processes')->body,
                name => 'four_processes',
            }
        ]
    }
);

$driver->add_job(
    {
        max_processes => 6,
        min_processes => 2,
        name          => 'sleeper',
        worker        => $w1,
        methods       => [
            {
                body => $w1->meta->find_method_by_name('sleeper')->body,
                name => 'sleeper',
            }
        ]
    }
);

$driver->run;
