package    # hide from PAUSE
  Live::NS2::UseBase;

use base qw(Base::TestWorker);
use Moose;

sub job : Job {
    my ( $self, $job, $workload ) = @_;
    open my $fh, ">>$workload" or die "cannot open file $workload: $!";
    print $fh "job ...\n";
    close $fh;
}

1;