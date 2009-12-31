use strict;
use warnings;
use Test::More tests => 14;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;
use File::Slurp;
use File::Temp qw(tempfile);

my $test = TestLib->new();
my $gc   = $test->gearman_client;

$test->run_gearmand;
$test->run_gearman_driver;

# give gearmand + driver at least 5 seconds to settle
for ( 1 .. 5 ) {
    my ( $ret, $pong ) = $gc->do( 'Live::NS1::Wrk1::ping' => 'ping' );
    sleep(1) && next unless $pong;
    is( $pong, 'pong', 'Job "Live::NS1::Wrk1::ping" returned correct value' );
    last;
}

{
    my ( $ret, $pong ) = $gc->do( 'something_custom_ping' => 'ping' );
    is( $pong, 'p0nG', 'Job "something_custom_ping" returned correct value' );
}

{
    my ( $ret, $pong ) = $gc->do( 'Live::NS2::Wrk2::ping' => 'ping' );
    is( $pong, 'PONG', 'Job "Live::NS2::Wrk2::ping" returned correct value' );
}

{
    my ( $ret, $pid ) = $gc->do( 'Live::NS1::Wrk1::get_pid' => '' );
    like( $pid, qr~^\d+$~, 'Job "get_pid" returned correct value' );
}

{
    $gc->do_background( 'Live::NS1::Wrk1::sleeper' => '5:' . time ) for 1 .. 5;    # blocks 5/6 slots for 5 secs

    my ( $ret, $time ) = $gc->do( 'Live::NS1::Wrk1::sleeper' => '0:' . time );
    ok( $time <= 2, 'Job "sleeper" returned in less than 2 seconds' );
}

{
    $gc->do_background( 'Live::NS1::Wrk1::sleeper' => '4:' . time );               # block last slot for another 4 secs

    my ( $ret, $time ) = $gc->do( 'Live::NS1::Wrk1::sleeper' => '0:' . time );
    ok( $time >= 2, 'Job "sleeper" returned in more than 2 seconds' );
}

{
    my ( $ret, $filename ) = $gc->do( 'Live::NS1::WrkBeginEnd::job' => 'some workload ...' );
    my $text = read_file($filename);
    is(
        $text,
        "begin some workload ...\njob some workload ...\nend some workload ...\n",
        'Begin/end blocks in worker have been run'
    );
    unlink $filename;
}

{
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Spread::main' => 'some workload ...' );
    is( $string, '12345', 'Spreading works (tests $worker->server attribute)' );
}

{
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Encode::job1' => 'some workload ...' );
    is( $string, 'STANDARDENCODE::some workload ...::STANDARDENCODE', 'Standard encoding works' );
}

{
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Encode::job2' => 'some workload ...' );
    is( $string, 'CUSTOMENCODE::some workload ...::CUSTOMENCODE', 'Custom encoding works' );
}

{
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Decode::job1' => 'some workload ...' );
    is( $string, 'STANDARDDECODE::some workload ...::STANDARDDECODE', 'Standard decoding works' );
}

{
    my ( $fh, $filename ) = tempfile( CLEANUP => 1 );
    my ( $ret, $nothing ) = $gc->do_background( 'Live::NS2::WrkBeginEnd::job' => $filename );
    sleep(2);
    my $text = read_file($filename);
    is(
        $text,
        "begin ...\nend ...\n",
        'Begin/end blocks in worker have been run, even if the job dies'
    );
}

{
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Decode::job2' => 'some workload ...' );
    is( $string, 'CUSTOMDECODE::some workload ...::CUSTOMDECODE', 'Custom decoding works' );
}

{
    my ( $fh, $filename ) = tempfile( CLEANUP => 1 );
    my ( $ret, $nothing ) = $gc->do( 'Live::NS2::UseBase::job' => $filename );
    my $text = read_file($filename);
    is(
        $text,
        "begin ...\njob ...\nend ...\n",
        'Begin/end blocks in worker base class have been run'
    );
}
