package    # hide from PAUSE
  Live::NS1::Decode;

use base qw(Gearman::Driver::Worker);
use Moose;

sub job1 : Job : Decode {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub job2 : Job : Decode(custom_decode) {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub decode {
    my ( $self, $workload ) = @_;
    return "STANDARDDECODE::${workload}::STANDARDDECODE";
}

sub custom_decode {
    my ( $self, $workload ) = @_;
    return "CUSTOMDECODE::${workload}::CUSTOMDECODE";
}

1;
