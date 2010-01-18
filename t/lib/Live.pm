package    # hide from PAUSE
  Live;

use base qw(Gearman::Driver::Worker);
use Moose;

sub job : Job {
    my ( $self, $job, $workload ) = @_;
    return 'ok';
}

1;
