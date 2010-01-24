use strict;
use warnings;
use Test::More tests => 45;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;
use File::Slurp;
use File::Temp qw(tempfile);
use Net::Telnet;

my $test = TestLib->new();
my $gc   = $test->gearman_client;

$test->run_gearmand;
$test->run_gearman_driver;

# give gearmand + driver at least 5 seconds to settle
foreach my $namespace (qw(Live::NS1::Basic Live::NS1::BasicChilds)) {
    for ( 1 .. 5 ) {
        my ( $ret, $pong ) = $gc->do( "${namespace}::ping" => 'ping' );
        sleep(1) && next unless $pong;
        is( $pong, 'pong', "Job '${namespace}::ping' returned correct value" );
        last;
    }
}

{
    my ( $ret, $pong ) = $gc->do( 'something_custom_ping' => 'ping' );
    is( $pong, 'p0nG', 'Job "something_custom_ping" returned correct value' );
}

{
    my ( $ret, $pong ) = $gc->do( 'Live::NS2::Ping2::ping' => 'ping' );
    is( $pong, 'PONG', 'Job "Live::NS2::Ping2::ping" returned correct value' );
}

# i hope this assumption is always true:
# out of 10000 jobs all 10 processes handled at least one job
{
    foreach my $namespace (qw(Live::NS1::Basic Live::NS1::BasicChilds)) {
        my %pids = ();
        for ( 1 .. 10000 ) {
            my ( $ret, $pid ) = $gc->do( "${namespace}::ten_processes" => '' );
            $pids{$pid}++;
            last if scalar( keys(%pids) ) == 10;
        }
        is( scalar( keys(%pids) ), 10, "10 different processes handled job 'ten_processes'" );
    }
}

# Let's change min/max processes via console
{
    my $telnet = Net::Telnet->new(
        Timeout => 5,
        Host    => '127.0.0.1',
        Port    => 47300,
    );
    $telnet->open;
    $telnet->print('set_min_processes Live::NS1::Basic::ten_processes 5');
    $telnet->print('set_max_processes Live::NS1::Basic::ten_processes 5');
    my %pids = ();
    for ( 1 .. 10000 ) {
        my ( $ret, $pid ) = $gc->do( 'Live::NS1::Basic::ten_processes' => '' );
        $pids{$pid}++;
        last if scalar( keys(%pids) ) == 5;
    }
    is( scalar( keys(%pids) ), 5, "5 different processes handled job 'ten_processes'" );
    $telnet->print('set_min_processes Live::NS1::Basic::ten_processes 10');
    $telnet->print('set_max_processes Live::NS1::Basic::ten_processes 10');
    $telnet->print('quit');
    $telnet->close;
}

{
    my ( $ret, $pid ) = $gc->do( 'Live::NS1::Basic::get_pid' => '' );
    like( $pid, qr~^\d+$~, 'Job "get_pid" returned correct value' );
}

{
    foreach my $namespace (qw(Live::NS1::Basic Live::NS1::BasicChilds)) {
        {
            $gc->do_background( "${namespace}::sleeper" => '5:' . time ) for 1 .. 5;    # blocks 5/6 slots for 5 secs

            my ( $ret, $time ) = $gc->do( "${namespace}::sleeper" => '0:' . time );
            ok( $time <= 2, 'Job "sleeper" returned in less than 2 seconds' );
        }
        {
            $gc->do_background( "${namespace}::sleeper" => '4:' . time );    # block last slot for another 4 secs

            my ( $ret, $time ) = $gc->do( "${namespace}::sleeper" => '0:' . time );
            ok( $time >= 2, "Job '${namespace}::sleeper' returned in more than 2 seconds" );
        }
    }
}

{
    my ( $ret, $filename ) = $gc->do( 'Live::NS1::BeginEnd::job' => 'some workload ...' );
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
    my ( $ret, $nothing ) = $gc->do_background( 'Live::NS2::BeginEnd::job' => $filename );
    sleep(2);
    my $text = read_file($filename);
    is( $text, "begin ...\nend ...\n", 'Begin/end blocks in worker have been run, even if the job dies' );
    unlink $filename;
}

{
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Decode::job2' => 'some workload ...' );
    is( $string, 'CUSTOMDECODE::some workload ...::CUSTOMDECODE', 'Custom decoding works' );
}

{
    my ( $fh, $filename ) = tempfile( CLEANUP => 1 );
    my ( $ret, $nothing ) = $gc->do( 'Live::NS2::UseBase::job' => $filename );
    my $text = read_file($filename);
    is( $text, "begin ...\njob ...\nend ...\n", 'Begin/end blocks in worker base class have been run' );
    unlink $filename;
}

{
    my @nothing = $gc->do_background( 'Live::NS1::Basic::quit' => 'exit' );
    sleep(3);    # wait for the worker being restarted
    my ( $ret, $string ) = $gc->do( 'Live::NS1::Basic::quit' => 'foo' );
    is( $string, 'i am back', 'Worker process restarted after exit' );
}

{
    foreach my $namespace (
        qw(
        Live::NS1::DefaultAttributes
        Live::NS1::DefaultAttributesChilds
        Live::NS1::OverrideAttributes
        Live::NS1::OverrideAttributesChilds
        )
      )
    {
        for ( 1 .. 3 ) {
            my ( $ret, $string ) = $gc->do( "${namespace}::job$_" => 'workload' );
            is(
                $string,
                "${namespace}::ENCODE::${namespace}::DECODE::workload::DECODE::${namespace}::ENCODE::${namespace}",
                'Encode/decode override attributes'
            );
        }
    }
}

{
    my ( $ret, $string ) = $gc->do( 'Live::job' => 'some workload ...' );
    is( $string, 'ok', 'loaded root namespace' );
}

#
#
# AddJob
#
#

foreach my $namespace (qw(Live::NS3::AddJob Live::NS3::AddJobChilds)) {
    {
        my ( $ret, $string ) = $gc->do( "${namespace}::job1" => 'foo' );
        is( $string, 'CUSTOMENCODE::foo::CUSTOMENCODE', 'Custom encoding works' );
    }

    {
        my ( $ret, $filename ) = $gc->do( "${namespace}::begin_end" => 'some workload ...' );
        my $text = read_file($filename);
        is(
            $text,
            "begin some workload ...\njob some workload ...\nend some workload ...\n",
            'Begin/end blocks in worker have been run'
        );
        unlink $filename;
    }

    # i hope this assumption is always true:
    # out of 10000 jobs all 10 processes handled at least one job
    {
        my %pids = ();
        for ( 1 .. 10000 ) {
            my ( $ret, $pid ) = $gc->do( "${namespace}::ten_processes" => 'xxx' );
            $pids{$pid}++;
            last if scalar( keys(%pids) ) == 10;
        }
        is( scalar( keys(%pids) ), 10, "10 different processes handled job '{namespace}::ten_processes'" );
    }

    {
        $gc->do_background( "${namespace}::sleeper" => '5:' . time ) for 1 .. 5;    # blocks 5/6 slots for 5 secs

        my ( $ret, $time ) = $gc->do( "${namespace}::sleeper" => '0:' . time );
        ok( $time <= 2, 'Job "Live::NS3::AddJob::sleeper" returned in less than 2 seconds' );
    }

    {
        $gc->do_background( "${namespace}::sleeper" => '4:' . time );               # block last slot for another 4 secs

        my ( $ret, $time ) = $gc->do( "${namespace}::sleeper" => '0:' . time );
        ok( $time >= 2, 'Job "Live::NS3::AddJob::sleeper" returned in more than 2 seconds' );
    }
}

{
    sleep(3);
    my @expected = (
        "Live::NS1::BasicChilds::ping\t1\t1\t1",
        "Live::NS2::Ping2::ping\t1\t1\t1",
        "Live::NS1::BeginEnd::job\t1\t1\t1",
        "Live::NS1::DefaultAttributesChilds::job2\t3\t1\t3",
        "Live::NS3::AddJob::begin_end\t1\t1\t1",
        "Live::NS1::Spread::some_job_4\t1\t1\t1",
        "Live::NS1::DefaultAttributes::job1\t3\t1\t3",
        "Live::NS1::Spread::some_job_1\t1\t1\t1",
        "Live::NS1::OverrideAttributes::job3\t1\t1\t1",
        "Live::NS2::UseBase::job\t1\t1\t1",
        "Live::NS1::Spread::main\t1\t1\t1",
        "Live::NS1::DefaultAttributesChilds::job3\t3\t1\t3",
        "Live::NS1::OverrideAttributesChilds::job2\t1\t1\t1",
        "Live::NS1::OverrideAttributes::job1\t1\t1\t1",
        "Live::NS1::Encode::job1\t1\t1\t1",
        "Live::NS1::Basic::quit\t1\t1\t1",
        "Live::NS1::Basic::ping\t1\t1\t1",
        "Live::NS1::Decode::job2\t1\t1\t1",
        "Live::NS1::Basic::get_pid\t1\t1\t1",
        "Live::NS1::DefaultAttributesChilds::job1\t3\t1\t3",
        "Live::NS1::DefaultAttributes::job2\t3\t1\t3",
        "Live::NS3::AddJobChilds::ten_processes\t10\t10\t10",
        "Live::NS1::Basic::sleeper\t2\t6\t2",
        "something_custom_ping\t1\t1\t1",
        "Live::NS1::Encode::job2\t1\t1\t1",
        "Live::NS2::BeginEnd::job\t1\t1\t1",
        "Live::NS3::AddJob::ten_processes\t10\t10\t10",
        "Live::NS1::Decode::job1\t1\t1\t1",
        "Live::NS1::OverrideAttributesChilds::job1\t1\t1\t1",
        "Live::NS1::BasicChilds::ten_processes\t10\t10\t10",
        "Live::NS3::AddJobChilds::sleeper\t2\t6\t6",
        "Live::NS1::OverrideAttributes::job2\t1\t1\t1",
        "Live::NS1::Spread::some_job_3\t1\t1\t1",
        "Live::NS1::Spread::some_job_2\t1\t1\t1",
        "Live::job\t1\t1\t1",
        "Live::NS1::OverrideAttributesChilds::job3\t1\t1\t1",
        "Live::NS1::Spread::some_job_5\t1\t1\t1",
        "Live::NS3::AddJobChilds::job1\t1\t5\t1",
        "Live::NS3::AddJob::job1\t1\t5\t1",
        "Live::NS1::DefaultAttributes::job3\t3\t1\t3",
        "Live::NS1::Basic::ten_processes\t5\t10\t5",
        "Live::NS3::AddJob::sleeper\t2\t6\t2",
        "Live::NS1::BasicChilds::sleeper\t2\t6\t2",
        "Live::NS3::AddJobChilds::begin_end\t1\t1\t1",
    );
    my $telnet = Net::Telnet->new(
        Timeout => 10,
        Host    => '127.0.0.1',
        Port    => 47300,
    );
    $telnet->open;
    $telnet->print('status');
    my @lines = ();
    while ( my $line = $telnet->getline() ) {
        last if $line eq ".\n";
        chomp $line;
        push @lines, $line;
    }
    $telnet->print('quit');
    $telnet->close;
	is_deeply(\@lines, \@expected);
}
