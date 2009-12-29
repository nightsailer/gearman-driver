package    # hide from PAUSE
  Live::NS2::Wrk2;

use base qw(Gearman::Driver::Worker);
use Moose;

sub ping : Job {
    return 'PONG';
}

1;
