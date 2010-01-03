package    # hide from PAUSE
  Validate::Valid::NS1::Something;

use base qw(Gearman::Driver::Worker);
use Moose;

sub bar : Job {
    my ( $self, $job, $workload ) = @_;
}

1;
