package    # hide from PAUSE
  GDExamples::Sleeper;

use base qw(Gearman::Driver::Worker);
use Moose;

sub ZzZzZzzz : Job : MinChilds(5) : MaxChilds(10) {
    my ( $self, $driver, $job ) = @_;
    my $time = 2;
    sleep($time);
    $self->output( $job->workload );
}

sub output {
    my ( $self, $workload ) = @_;
    print "$workload\n";
}

1;
