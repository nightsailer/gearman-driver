use strict;
use warnings;
use Test::More tests => 260;
use Test::Differences;
use FindBin;
use lib "$FindBin::Bin/lib";
use TestLib;
use Net::Telnet;

my $test = TestLib->new();
my $gc   = $test->gearman_client;

$test->run_gearmand;
$test->run_gearman_driver;

my $telnet = $test->telnet_client;

my @job_names = ();

sleep(5);

{
    my @expected = (
        "Live::NS1::Basic::get_pid                  0   1   0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::ping                     1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::quit                     1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::sleeper                  2   6   2  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::sleepy_pid               0   1   0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::ten_processes           10  10  10  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::ping               1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::sleeper            2   6   2  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::ten_processes     10  10  10  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BeginEnd::job                   1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Decode::job1                    1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Decode::job2                    1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::DefaultAttributes::job          3   1   3  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::DefaultAttributesChilds::job    3   1   3  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Encode::job1                    1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Encode::job2                    1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::OverrideAttributes::job         1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::OverrideAttributesChilds::job   1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::main                    1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_1              1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_2              1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_3              1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_4              1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_5              1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::BeginEnd::job                   1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::Ping2::ping                     1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::UseBase::job                    1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::begin_end               1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::job1                    1   5   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::sleeper                 2   6   2  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::ten_processes          10  10  10  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::begin_end         1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::job1              1   5   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::sleeper           2   6   2  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::ten_processes    10  10  10  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::job                                  1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "something_custom_ping                      1   1   1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
    );
    $telnet->print('status');
    my @lines = ();
    while ( my $line = $telnet->getline() ) {
        last if $line eq ".\n";
        chomp $line;
        my ($job_name) = $line =~ /(.*?)\s+/;
        push @job_names, $job_name;
        push @lines,     $line;
    }
    eq_or_diff( \@lines, \@expected );
}

{
    my @pids = ();
    my $test = sub {
        my @expected = ( qr/^Live::NS1::BasicChilds::sleeper  2  6  2$/, qr/^\d+$/, qr/^\d+$/ );
        $telnet->print('show Live::NS1::BasicChilds::sleeper');
        while ( my $line = $telnet->getline() ) {
            last if $line eq ".\n";
            chomp $line;
            like( $line, shift(@expected) );
            push @pids, $line if $line =~ /^\d+$/;
        }
    };

    $test->();
    $telnet->print('kill 1');
    is( $telnet->getline(), "ERR invalid_value: the given PID(s) do not belong to us\n" );

    my @old_pids = @pids;

    $telnet->print( 'kill ' . shift(@pids) );
    is( $telnet->getline(), "OK\n" );
    is( $telnet->getline(), ".\n" );

    $telnet->print( 'kill ' . shift(@pids) );
    is( $telnet->getline(), "OK\n" );
    is( $telnet->getline(), ".\n" );

    sleep(6);

    $test->();

    is( scalar(@pids), scalar(@old_pids) );

    for ( 0 .. 1 ) {
        isnt( shift(@pids), shift(@old_pids) );
    }

    $telnet->print('killall Live::NS1::BasicChilds::sleeper');
    is( $telnet->getline(), "OK\n" );
    is( $telnet->getline(), ".\n" );

    sleep(6);

    $test->();

    is( scalar(@pids), 2 );
}

{
    foreach my $job_name (@job_names) {
        $telnet->print("set_min_processes $job_name 0");
        is( $telnet->getline(), "OK\n" );
        is( $telnet->getline(), ".\n" );
        $telnet->print("set_max_processes $job_name 1");
        is( $telnet->getline(), "OK\n" );
        is( $telnet->getline(), ".\n" );
    }
    sleep(6);

    my @expected = (
        "Live::NS1::Basic::get_pid                 0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::ping                    0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::quit                    0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::sleeper                 0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::sleepy_pid              0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::ten_processes           0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::ping              0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::sleeper           0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::ten_processes     0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BeginEnd::job                  0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Decode::job1                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Decode::job2                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::DefaultAttributes::job         0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::DefaultAttributesChilds::job   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Encode::job1                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Encode::job2                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::OverrideAttributes::job        0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::OverrideAttributesChilds::job  0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::main                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_1             0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_2             0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_3             0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_4             0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_5             0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::BeginEnd::job                  0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::Ping2::ping                    0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::UseBase::job                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::begin_end              0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::job1                   0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::sleeper                0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::ten_processes          0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::begin_end        0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::job1             0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::sleeper          0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::ten_processes    0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::job                                 0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "something_custom_ping                     0  1  0  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
    );

    $telnet->print('status');
    my @lines = ();
    while ( my $line = $telnet->getline() ) {
        last if $line eq ".\n";
        chomp $line;
        push @lines, $line;
    }
    eq_or_diff( \@lines, \@expected );

    foreach my $job_name (@job_names) {
        $telnet->print("set_processes $job_name 1 1");
        is( $telnet->getline(), "OK\n" );
        is( $telnet->getline(), ".\n" );
    }
    sleep(6);

    @expected = (
        "Live::NS1::Basic::get_pid                 1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::ping                    1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::quit                    1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::sleeper                 1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::sleepy_pid              1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Basic::ten_processes           1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::ping              1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::sleeper           1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BasicChilds::ten_processes     1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::BeginEnd::job                  1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Decode::job1                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Decode::job2                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::DefaultAttributes::job         1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::DefaultAttributesChilds::job   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Encode::job1                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Encode::job2                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::OverrideAttributes::job        1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::OverrideAttributesChilds::job  1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::main                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_1             1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_2             1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_3             1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_4             1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS1::Spread::some_job_5             1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::BeginEnd::job                  1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::Ping2::ping                    1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS2::UseBase::job                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::begin_end              1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::job1                   1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::sleeper                1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJob::ten_processes          1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::begin_end        1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::job1             1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::sleeper          1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::NS3::AddJobChilds::ten_processes    1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "Live::job                                 1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
        "something_custom_ping                     1  1  1  1970-01-01T00:00:00  1970-01-01T00:00:00   ",
    );

    $telnet->print('status');
    @lines = ();
    while ( my $line = $telnet->getline() ) {
        last if $line eq ".\n";
        chomp $line;
        push @lines, $line;
    }
    eq_or_diff( \@lines, \@expected );
}

{
    $telnet->print("asdf");
    is( $telnet->getline(), "ERR unknown_command: asdf\n" );

    $telnet->print("set_min_processes asdf 5");
    is( $telnet->getline(), "ERR invalid_job_name: asdf\n" );

    $telnet->print("set_min_processes Live::job ten");
    is( $telnet->getline(), "ERR invalid_value: min_processes must be >= 0\n" );

    $telnet->print("set_min_processes Live::job 10");
    is( $telnet->getline(), "ERR invalid_value: min_processes must be smaller than max_processes\n" );

    $telnet->print("set_max_processes asdf 5");
    is( $telnet->getline(), "ERR invalid_job_name: asdf\n" );

    $telnet->print("set_max_processes Live::job ten");
    is( $telnet->getline(), "ERR invalid_value: max_processes must be >= 0\n" );

    $telnet->print("set_max_processes Live::job 5");
    is( $telnet->getline(), "OK\n" );
    is( $telnet->getline(), ".\n" );
    $telnet->print("set_min_processes Live::job 5");
    is( $telnet->getline(), "OK\n" );
    is( $telnet->getline(), ".\n" );

    $telnet->print("set_max_processes Live::job 4");
    is( $telnet->getline(), "ERR invalid_value: max_processes must be greater than min_processes\n" );

    $telnet->print("set_processes asdf 1 1");
    is( $telnet->getline(), "ERR invalid_job_name: asdf\n" );

    $telnet->print("set_processes Live::job ten ten");
    is( $telnet->getline(), "ERR invalid_value: min_processes must be >= 0\n" );

    $telnet->print("set_processes Live::job 1 ten");
    is( $telnet->getline(), "ERR invalid_value: max_processes must be >= 0\n" );

    $telnet->print("set_processes Live::job 5 1");
    is( $telnet->getline(), "ERR invalid_value: max_processes must be greater than min_processes\n" );
}

$telnet->print(' shutdown ');
