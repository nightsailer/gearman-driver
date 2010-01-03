package    # hide from PAUSE
  Validate::Valid::NS2::Something;

use base qw(Gearman::Driver::Worker);
use Moose;

sub bla : Job {
    my ( $self, $job, $workload ) = @_;
}

1;
