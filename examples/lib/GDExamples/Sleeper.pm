package    # hide from PAUSE
  GDExamples::Sleeper;

use base qw(Gearman::Driver::Worker);
use Moose;

sub process_name {
    my ( $self, $orig, $job_name ) = @_;
    return "$orig ($job_name)";
}

sub begin {
    my ( $self, $job, $workload ) = @_;
    print "$workload begin called...\n";
}

sub end {
    my ( $self, $job, $workload ) = @_;
    print "$workload end called...\n";
}

sub ZzZzZzzz : Job : MinChilds(3) : MaxChilds(6) {
    my ( $self, $job, $workload ) = @_;
    my $time = 2;
    sleep($time);
    $self->output( $job->workload . " job called..." );
}

sub output {
    my ( $self, $workload ) = @_;
    print "$workload\n";
}

sub long_running_ZzZzZzzz : Job : MinChilds(1) : MaxChilds(2) {
    my ( $self, $job, $workload ) = @_;
    my $time = 4;
    sleep($time);
    $self->output( $job->workload );
}

1;
