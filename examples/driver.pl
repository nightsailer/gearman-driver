#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib ( "$FindBin::Bin/../lib", "$FindBin::Bin/lib" );
use Gearman::Driver;

# /usr/local/sbin/gearmand -d
# ./examples/driver.pl --loglevel DEBUG &
# tail -f gearman_driver.log &
# ./examples/client.pl

my $driver = Gearman::Driver->new_with_options(
    namespaces           => [qw(GDExamples)],
    server               => 'localhost:4730',
    interval             => 2,
    unknown_job_callback => sub {
        my ( $driver, $status ) = @_;
        warn "UNKNOWN JOB:";
        use Data::Dumper;
        $Data::Dumper::Sortkeys=1;
        warn Dumper $status;
    }
);
$driver->run;
