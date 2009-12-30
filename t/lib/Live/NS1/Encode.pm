package    # hide from PAUSE
  Live::NS1::Encode;

use base qw(Gearman::Driver::Worker);
use Moose;

sub job1 : Job : Encode {
    my ( $self, $job, $workload ) = @_;
    return $job->workload;
}

sub job2 : Job : Encode(custom_encode) {
    my ( $self, $job, $workload ) = @_;
    return $job->workload;
}

sub encode {
    my ( $self, $result ) = @_;
    return "STANDARDENCODE::${result}::STANDARDENCODE";
}

sub custom_encode {
    my ( $self, $result ) = @_;
    return "CUSTOMENCODE::${result}::CUSTOMENCODE";
}

1;
