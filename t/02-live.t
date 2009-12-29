use strict;
use warnings;
use Test::More tests => 2;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;

my $test = TestLib->new();
my $gc   = $test->gearman_client;

$test->run_gearmand;
$test->run_gearman_driver;

# give gearmand + driver at least 5 seconds to settle
for ( 1 .. 5 ) {
    my ( $ret, $pong ) = $gc->do( 'Live::NS1::Wrk1::ping' => 'ping' );
    sleep(1) && next unless $pong;
    is( $pong, 'pong', 'Job "ping" returned "pong"' );
    last;
}

{
    my ( $ret, $pid ) = $gc->do( 'Live::NS1::Wrk1::get_pid' => '' );
    like( $pid, qr~^\d+$~, 'Job "get_pid" returned some number' );
}
