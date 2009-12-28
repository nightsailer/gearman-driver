#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib ( "$FindBin::Bin/../lib", "$FindBin::Bin/lib" );
use Gearman::Driver;

my $driver = Gearman::Driver->new_with_options(
    namespaces => [qw(GDExamples)],
    server     => 'localhost:4730,localhost:4731',
    interval   => 1,
);
$driver->run;
