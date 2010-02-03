package GDBenchmark;

use base qw(Gearman::Driver::Worker);
use Moose;

sub ping : Job : MinProcesses(1) : MaxProcesses(1) {
    my ( $self, $job, $workload ) = @_;
    return "pong";
}

1;
