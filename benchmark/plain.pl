#!/usr/bin/env perl
use strict;
use warnings;
use Gearman::XS::Worker;
use Gearman::XS qw(:constants);
use GDBenchmark;

my $gdb    = GDBenchmark->new();
my $worker = Gearman::XS::Worker->new;
$worker->add_server( 'localhost', 4730 );

$worker->add_function( "ping", 0, sub { return $gdb->ping() }, {} );

while (1) {
    my $ret = $worker->work();
    if ( $ret != GEARMAN_SUCCESS ) {
        printf( STDERR "%s\n", $worker->error() );
    }
}
