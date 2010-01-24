use strict;
use warnings;
use Test::More tests => 1;
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

{
    sleep(5);
    my @expected = (
        "Live::NS1::BasicChilds::ping\t1\t1\t1",              "Live::NS2::Ping2::ping\t1\t1\t1",
        "Live::NS1::BeginEnd::job\t1\t1\t1",                  "Live::NS1::DefaultAttributesChilds::job2\t3\t1\t3",
        "Live::NS3::AddJob::begin_end\t1\t1\t1",              "Live::NS1::Spread::some_job_4\t1\t1\t1",
        "Live::NS1::DefaultAttributes::job1\t3\t1\t3",        "Live::NS1::Spread::some_job_1\t1\t1\t1",
        "Live::NS1::OverrideAttributes::job3\t1\t1\t1",       "Live::NS2::UseBase::job\t1\t1\t1",
        "Live::NS1::Spread::main\t1\t1\t1",                   "Live::NS1::DefaultAttributesChilds::job3\t3\t1\t3",
        "Live::NS1::OverrideAttributesChilds::job2\t1\t1\t1", "Live::NS1::OverrideAttributes::job1\t1\t1\t1",
        "Live::NS1::Encode::job1\t1\t1\t1",                   "Live::NS1::Basic::quit\t1\t1\t1",
        "Live::NS1::Basic::ping\t1\t1\t1",                    "Live::NS1::Decode::job2\t1\t1\t1",
        "Live::NS1::Basic::get_pid\t1\t1\t1",                 "Live::NS1::DefaultAttributesChilds::job1\t3\t1\t3",
        "Live::NS1::DefaultAttributes::job2\t3\t1\t3",        "Live::NS3::AddJobChilds::ten_processes\t10\t10\t10",
        "Live::NS1::Basic::sleeper\t2\t6\t2",                 "something_custom_ping\t1\t1\t1",
        "Live::NS1::Encode::job2\t1\t1\t1",                   "Live::NS2::BeginEnd::job\t1\t1\t1",
        "Live::NS3::AddJob::ten_processes\t10\t10\t10",       "Live::NS1::Decode::job1\t1\t1\t1",
        "Live::NS1::OverrideAttributesChilds::job1\t1\t1\t1", "Live::NS1::BasicChilds::ten_processes\t10\t10\t10",
        "Live::NS3::AddJobChilds::sleeper\t2\t6\t2",          "Live::NS1::OverrideAttributes::job2\t1\t1\t1",
        "Live::NS1::Spread::some_job_3\t1\t1\t1",             "Live::NS1::Spread::some_job_2\t1\t1\t1",
        "Live::job\t1\t1\t1",                                 "Live::NS1::OverrideAttributesChilds::job3\t1\t1\t1",
        "Live::NS1::Spread::some_job_5\t1\t1\t1",             "Live::NS3::AddJobChilds::job1\t1\t5\t1",
        "Live::NS3::AddJob::job1\t1\t5\t1",                   "Live::NS1::DefaultAttributes::job3\t3\t1\t3",
        "Live::NS1::Basic::ten_processes\t10\t10\t10",        "Live::NS3::AddJob::sleeper\t2\t6\t2",
        "Live::NS1::BasicChilds::sleeper\t2\t6\t2",           "Live::NS3::AddJobChilds::begin_end\t1\t1\t1",
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

$telnet->print('shutdown');
