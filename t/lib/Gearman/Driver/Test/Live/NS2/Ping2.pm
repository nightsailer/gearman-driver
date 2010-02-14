package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS2::Ping2;

use base qw(Gearman::Driver::Test::Base::All);
use Moose;

sub ping : Job {
    return 'PONG';
}

1;
