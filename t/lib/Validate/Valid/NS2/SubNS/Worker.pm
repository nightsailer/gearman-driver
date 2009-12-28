package    # hide from PAUSE
  Validate::Valid::NS2::SubNS::Worker;

use base qw(Gearman::Driver::Worker);
use Moose;

sub fasel : Job {
    my ( $self, $data ) = @_;
}

1;