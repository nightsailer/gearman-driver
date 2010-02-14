package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS1::OverrideAttributes;

use base qw(Gearman::Driver::Test::Base::All);
use Moose;

sub override_attributes {
    return {
        MinProcesses => 1,
        Encode       => 'encode',
        Decode       => 'decode',
    };
}

sub job : Job : MinProcesses(5) : Encode(invalid) : Decode(invalid) {
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
