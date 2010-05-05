use strict;
use warnings;
use Test::More tests => 2;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver::Test;
use File::Slurp;
use File::Temp qw(tempfile);

my $test = Gearman::Driver::Test->new();
my $gc   = $test->gearman_client;

$test->prepare('--namespaces Gearman::Driver::Test::Live::RuntimeOption',"--configfile $FindBin::Bin/gearman_driver.yml");

my $job1 = 'Gearman::Driver::Test::Live::RuntimeOption::job1';
{
    # my ( $fh, $filename ) = tempfile( CLEANUP => 1 );
    my $result_ref = $gc->do_task( $job1 );
    # my $text = read_file($filename);
    is( ${$result_ref}, "Test", 'worker attribute Foo runtime changed to Test' );
}

{
    my $telnet = $test->telnet_client;
    $telnet->print("status");
    while ( my $line = $telnet->getline() ) {
        last if $line eq ".\n";
        chomp $line;
        like( $line, qr/^$job1  2  10  2 .*$/,'job runtime attributes' );
    }
}
$test->shutdown;
