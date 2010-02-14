package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS2::Ping1;

use base qw(Gearman::Driver::Test::Base::All);
use Moose;

sub prefix { 'something_custom_' }

sub ping : Job {
    return 'p0nG';
}

1;
