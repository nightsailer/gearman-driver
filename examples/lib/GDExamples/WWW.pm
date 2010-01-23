package    # hide from PAUSE
  GDExamples::WWW;

use base qw(Gearman::Driver::Worker);
use Moose;
use LWP::UserAgent;

has 'ua' => (
    default => sub { LWP::UserAgent->new },
    is      => 'ro',
    isa     => 'LWP::UserAgent',
);

sub process_name {
    my ( $self, $orig, $job_name ) = @_;
    return "$orig ($job_name)";
}

sub is_online : Job : MinProcesses(0) {
    my ( $self, $job, $workload ) = @_;
    my $response = $self->ua->get($workload);
    printf "%s => %s\n", $workload, $response->status_line;
    return $response->is_success;
}

1;
