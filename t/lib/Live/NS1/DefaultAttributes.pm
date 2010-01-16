package    # hide from PAUSE
  Live::NS1::DefaultAttributes;

use base qw(Gearman::Driver::Worker);
use Moose;

sub default_attributes {
    return {
        MinChilds => 3,
        Encode    => 'encode',
        Decode    => 'decode',
    };
}

sub job1 : Job {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub job2 : Job {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub job3 : Job {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub encode {
    my ( $self, $result ) = @_;
    return "DefaultAttributes::ENCODE::${result}::ENCODE::DefaultAttributes";
}

sub decode {
    my ( $self, $workload ) = @_;
    return "DefaultAttributes::DECODE::${workload}::DECODE::DefaultAttributes";
}

1;
