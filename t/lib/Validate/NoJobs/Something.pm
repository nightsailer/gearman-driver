package    # hide from PAUSE
  Validate::NoJobs::Something;

use base qw(Gearman::Driver::Worker);
use Moose;

sub foo : FOO {
    my ( $self, $data ) = @_;
}

sub bar : BAR {
    my ( $self, $data ) = @_;
}

1;
