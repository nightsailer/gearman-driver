package    # hide from PAUSE
  Validate::Valid::NS1::Worker;

use base qw(Gearman::Driver::Worker);
use Moose;

sub bar : Job {
    my ( $self, $data ) = @_;
}

1;