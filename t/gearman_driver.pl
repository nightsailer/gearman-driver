#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;

my $driver = TestLib->gearman_driver;
$driver->run;
