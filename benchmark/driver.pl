#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Gearman::Driver;
Gearman::Driver->new_with_options( namespaces => [qw(GDBenchmark)] )->run;
