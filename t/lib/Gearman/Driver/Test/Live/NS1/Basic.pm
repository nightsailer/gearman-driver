package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS1::Basic;

use base qw(Gearman::Driver::Test::Base::All);
use Moose;

has 'four_processes_done' => (
    default => 0,
    is      => 'rw',
    isa     => 'Bool',
);

sub sleepy_pid : Job : MinProcesses(0) {
    my ( $self, $job, $workload ) = @_;
    $workload ||= 0;
    for ( 0 .. $workload ) {
        warn "sleeping zZzZz $_";
        sleep(1);
    }
    return $self->pid;
}

sub get_pid : Job : MinProcesses(0) {
    my ( $self, $job, $workload ) = @_;
    warn "get_pid: " . $self->pid;
    return $self->pid;
}

sub four_processes : Job : MinProcesses(4) : MaxProcesses(4) {
    my ( $self, $job, $workload ) = @_;
    if ( $self->four_processes_done ) {
        exit(1);
    }
    $self->four_processes_done(1);
    return $self->pid;
}

sub sleeper : Job : MinProcesses(2) : MaxProcesses(6) {
    my ( $self, $job, $workload ) = @_;
    my ( $sleep, $time ) = split /:/, $job->workload;
    sleep($sleep) if $sleep;
    return time - $time;
}

sub pid {
    return $$;
}

sub ping : Job : ProcessGroup(group1) {
    return 'pong';
}

sub pid1 : Job : ProcessGroup(group1) {
    my ( $self, $job, $workload ) = @_;
    return $self->pid;
}

sub pid2 : Job : ProcessGroup(group1) {
    my ( $self, $job, $workload ) = @_;
    return $self->pid;
}

sub quit : Job : ProcessGroup(group1) {
    my ( $self, $job, $workload ) = @_;
    exit(0) if $workload eq 'exit';
    return 'i am back';
}

1;
