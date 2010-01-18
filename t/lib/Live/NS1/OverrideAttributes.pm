package    # hide from PAUSE
  Live::NS1::OverrideAttributes;

use base qw(Gearman::Driver::Worker);
use Moose;

sub override_attributes {
    return {
        MinChilds => 1,
        Encode    => 'encode',
        Decode    => 'decode',
    };
}

sub job1 : Job : MinChilds(5) : Encode(invalid) : Decode(invalid) {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub job2 : Job : MinChilds(5) : Encode(invalid) : Decode(invalid) {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub job3 : Job : MinChilds(5) : Encode(invalid) : Decode(invalid) {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub encode {
    my ( $self, $result ) = @_;
    return "OverrideAttributes::ENCODE::${result}::ENCODE::OverrideAttributes";
}

sub decode {
    my ( $self, $workload ) = @_;
    return "OverrideAttributes::DECODE::${workload}::DECODE::OverrideAttributes";
}

1;
