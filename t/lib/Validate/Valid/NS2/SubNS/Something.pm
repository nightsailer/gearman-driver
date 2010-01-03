package    # hide from PAUSE
  Validate::Valid::NS2::SubNS::Something;

use base qw(Gearman::Driver::Worker);
use Moose;

sub fasel : Job {
    my ( $self, $job, $workload ) = @_;
}

1;
