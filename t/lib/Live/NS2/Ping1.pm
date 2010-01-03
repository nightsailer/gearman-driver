package    # hide from PAUSE
  Live::NS2::Ping1;

use base qw(Gearman::Driver::Worker);
use Moose;

sub prefix { 'something_custom_' }

sub ping : Job {
    return 'p0nG';
}

1;
