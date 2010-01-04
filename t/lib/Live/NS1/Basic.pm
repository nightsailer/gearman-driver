package    # hide from PAUSE
  Live::NS1::Basic;

use base qw(Gearman::Driver::Worker);
use Moose;

sub ping : Job {
    return 'pong';
}

sub get_pid : Job {
    my ( $self, $job, $workload ) = @_;
    return $self->pid;
}

sub ten_childs : Job : MinChilds(10) : MaxChilds(10) {
    my ( $self, $job, $workload ) = @_;
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

sub quit : Job {
    my ( $self, $job, $workload ) = @_;
    exit(0) if $workload eq 'exit';
    return 'i am back';
}

1;
