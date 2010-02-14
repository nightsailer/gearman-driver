package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS1::EncodeDecode;

use base qw(Gearman::Driver::Test::Base::All);
use Moose;

sub job1 : Job : Decode : ProcessGroup(group1) {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub job2 : Job : Decode(custom_decode) : ProcessGroup(group1) {
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

sub job3 : Job : Encode : ProcessGroup(group1) {
    my ( $self, $job, $workload ) = @_;
    return $job->workload;
}

sub job4 : Job : Encode(custom_encode) : ProcessGroup(group1) {
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
