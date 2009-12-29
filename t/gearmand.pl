#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;

my $server = TestLib->gearman_server;

$server->set_backlog(2);
$server->set_job_retries(3);
$server->set_threads(2);
$server->run();
