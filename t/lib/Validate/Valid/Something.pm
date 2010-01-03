package    # hide from PAUSE
  Validate::Valid::Something;

use base qw(Gearman::Driver::Worker);
use Moose;

sub foo {
    my ( $self, $job, $workload ) = @_;
}

1;
