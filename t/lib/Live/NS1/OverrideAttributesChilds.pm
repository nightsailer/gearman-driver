package    # hide from PAUSE
  Live::NS1::OverrideAttributesChilds;

use base qw(Gearman::Driver::Worker);
use Moose;

sub override_attributes {
    return {
        MinChilds => 1,
        Encode    => 'encode',
        Decode    => 'decode',
    };
}

sub job : Job : MinChilds(5) : Encode(invalid) : Decode(invalid) {
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
