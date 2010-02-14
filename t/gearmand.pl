#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver::Test;

my $server = Gearman::Driver::Test->gearman_server_run;
