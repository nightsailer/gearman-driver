#!/usr/bin/env perl
use strict;
use warnings;
use Gearman::XS::Client;
use Benchmark qw(:all) ;

my $client = Gearman::XS::Client->new();
$client->add_server( 'localhost', 4730 );

timethese(
    50000,
    {
        'plain Gearman::XS' => sub {
            my ( $ret, $pong ) = $client->do( "ping" => '' );
        },
        'Gearman::Driver' => sub {
            my ( $ret, $pong ) = $client->do( "GDBenchmark::ping" => '' );
        },
    }
);
