package    # hide from PAUSE
  Validate::NoJobs::NS1::Worker;

use base qw(Gearman::Driver::Worker);
use Moose;

sub bla : BLA {
    my ( $self, $data ) = @_;
}

sub fasel : FASEL {
    my ( $self, $data ) = @_;
}

1;
