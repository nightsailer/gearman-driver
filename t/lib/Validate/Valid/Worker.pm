package    # hide from PAUSE
  Validate::Valid::Worker;

use base qw(Gearman::Driver::Worker);
use Moose;

sub foo {
    my ( $self, $data ) = @_;
}

1;