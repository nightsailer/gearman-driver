use strict;
use warnings;
use Test::More tests => 157;
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
        "Live::NS1::BasicChilds::ping\t1\t1\t1",             "Live::NS2::Ping2::ping\t1\t1\t1",
        "Live::NS1::OverrideAttributesChilds::job\t1\t1\t1", "Live::NS1::OverrideAttributes::job\t1\t1\t1",
        "Live::NS1::BeginEnd::job\t1\t1\t1",                 "Live::NS3::AddJob::begin_end\t1\t1\t1",
        "Live::NS1::Spread::some_job_4\t1\t1\t1",            "Live::NS1::Spread::some_job_1\t1\t1\t1",
        "Live::NS2::UseBase::job\t1\t1\t1",                  "Live::NS1::Spread::main\t1\t1\t1",
        "Live::NS1::Encode::job1\t1\t1\t1",                  "Live::NS1::Basic::quit\t1\t1\t1",
        "Live::NS1::Basic::ping\t1\t1\t1",                   "Live::NS1::Decode::job2\t1\t1\t1",
        "Live::NS1::Basic::get_pid\t1\t1\t1",                "Live::NS3::AddJobChilds::ten_processes\t10\t10\t10",
        "Live::NS1::Basic::sleeper\t2\t6\t2",                "something_custom_ping\t1\t1\t1",
        "Live::NS1::DefaultAttributesChilds::job\t3\t1\t3",  "Live::NS1::Encode::job2\t1\t1\t1",
        "Live::NS2::BeginEnd::job\t1\t1\t1",                 "Live::NS3::AddJob::ten_processes\t10\t10\t10",
        "Live::NS1::Decode::job1\t1\t1\t1",                  "Live::NS1::DefaultAttributes::job\t3\t1\t3",
        "Live::NS1::BasicChilds::ten_processes\t10\t10\t10", "Live::NS3::AddJobChilds::sleeper\t2\t6\t2",
        "Live::NS1::Spread::some_job_2\t1\t1\t1",            "Live::NS1::Spread::some_job_3\t1\t1\t1",
        "Live::job\t1\t1\t1",                                "Live::NS3::AddJobChilds::job1\t1\t5\t1",
        "Live::NS3::AddJob::job1\t1\t5\t1",                  "Live::NS1::Spread::some_job_5\t1\t1\t1",
        "Live::NS1::Basic::ten_processes\t10\t10\t10",       "Live::NS3::AddJob::sleeper\t2\t6\t2",
        "Live::NS1::BasicChilds::sleeper\t2\t6\t2",          "Live::NS3::AddJobChilds::begin_end\t1\t1\t1",
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
        "Live::NS1::BasicChilds::ping\t0\t1\t0",             "Live::NS2::Ping2::ping\t0\t1\t0",
        "Live::NS1::OverrideAttributesChilds::job\t0\t1\t0", "Live::NS1::OverrideAttributes::job\t0\t1\t0",
        "Live::NS1::BeginEnd::job\t0\t1\t0",                 "Live::NS3::AddJob::begin_end\t0\t1\t0",
        "Live::NS1::Spread::some_job_4\t0\t1\t0",            "Live::NS1::Spread::some_job_1\t0\t1\t0",
        "Live::NS2::UseBase::job\t0\t1\t0",                  "Live::NS1::Spread::main\t0\t1\t0",
        "Live::NS1::Encode::job1\t0\t1\t0",                  "Live::NS1::Basic::quit\t0\t1\t0",
        "Live::NS1::Basic::ping\t0\t1\t0",                   "Live::NS1::Decode::job2\t0\t1\t0",
        "Live::NS1::Basic::get_pid\t0\t1\t0",                "Live::NS3::AddJobChilds::ten_processes\t0\t1\t0",
        "Live::NS1::Basic::sleeper\t0\t1\t0",                "something_custom_ping\t0\t1\t0",
        "Live::NS1::DefaultAttributesChilds::job\t0\t1\t0",  "Live::NS1::Encode::job2\t0\t1\t0",
        "Live::NS2::BeginEnd::job\t0\t1\t0",                 "Live::NS3::AddJob::ten_processes\t0\t1\t0",
        "Live::NS1::Decode::job1\t0\t1\t0",                  "Live::NS1::DefaultAttributes::job\t0\t1\t0",
        "Live::NS1::BasicChilds::ten_processes\t0\t1\t0",    "Live::NS3::AddJobChilds::sleeper\t0\t1\t0",
        "Live::NS1::Spread::some_job_2\t0\t1\t0",            "Live::NS1::Spread::some_job_3\t0\t1\t0",
        "Live::job\t0\t1\t0",                                "Live::NS3::AddJobChilds::job1\t0\t1\t0",
        "Live::NS3::AddJob::job1\t0\t1\t0",                  "Live::NS1::Spread::some_job_5\t0\t1\t0",
        "Live::NS1::Basic::ten_processes\t0\t1\t0",          "Live::NS3::AddJob::sleeper\t0\t1\t0",
        "Live::NS1::BasicChilds::sleeper\t0\t1\t0",          "Live::NS3::AddJobChilds::begin_end\t0\t1\t0",
    );

    $telnet->print('status');
    my @lines = ();
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
}

$telnet->print(' shutdown ');
