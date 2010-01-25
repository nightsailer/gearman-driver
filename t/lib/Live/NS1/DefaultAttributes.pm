package    # hide from PAUSE
  Live::NS1::DefaultAttributes;

use base qw(Gearman::Driver::Worker);
use Moose;

sub default_attributes {
    return {
        MinProcesses => 3,
        Encode       => 'encode',
        Decode       => 'decode',
    };
}

sub job : Job {
    my ( $self, $job, $workload ) = @_;
    return $workload;
}

sub encode {
    my ( $self, $result ) = @_;
    my $package = ref($self);
    return "${package}::ENCODE::${result}::ENCODE::${package}";
}

sub decode {
    my ( $self, $workload ) = @_;
    my $package = ref($self);
    return "${package}::DECODE::${workload}::DECODE::${package}";
}

1;
