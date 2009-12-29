package    # hide from PAUSE
  Live::NS1::Wrk1;

use base qw(Gearman::Driver::Worker);
use Moose;

sub ping : Job {
    return 'pong';
}

sub get_pid : Job {
    my ( $self, $job ) = @_;
    return $self->pid;
}

sub pid {
    return $$;
}

1;
