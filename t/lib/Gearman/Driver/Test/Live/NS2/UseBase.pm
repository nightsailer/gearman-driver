package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS2::UseBase;

use base qw(Gearman::Driver::Test::Base::TestWorker);
use Moose;

sub job : Job {
    my ( $self, $job, $workload ) = @_;
    open my $fh, ">>$workload" or die "cannot open file $workload: $!";
    print $fh "job ...\n";
    close $fh;
}

1;