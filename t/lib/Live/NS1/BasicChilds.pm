package    # hide from PAUSE
  Live::NS1::BasicChilds;

use base qw(Gearman::Driver::Worker);
use Moose;

has 'ten_processes_done' => (
    default => 0,
    is      => 'rw',
    isa     => 'Bool',
);

sub ping : Job {
    return 'pong';
}

sub ten_processes : Job : MinChilds(10) : MaxChilds(10) {
    my ( $self, $job, $workload ) = @_;
    if ( $self->ten_processes_done ) {
        exit(1);
    }
    $self->ten_processes_done(1);
    return $self->pid;
}

sub sleeper : Job : MinChilds(2) : MaxChilds(6) {
    my ( $self, $job, $workload ) = @_;
    my ( $sleep, $time ) = split /:/, $job->workload;
    sleep($sleep) if $sleep;
    return time - $time;
}

sub pid {
    return $$;
}

1;
