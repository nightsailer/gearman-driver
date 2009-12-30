package    # hide from PAUSE
  Live::NS1::WrkBeginEnd;

use base qw(Gearman::Driver::Worker);
use Moose;
use File::Temp qw(tempfile);

has 'filename' => ( is => 'ro' );
has 'fh'       => ( is => 'ro' );

sub begin {
    my ( $self, $job, $workload ) = @_;
    my ( $fh, $filename ) = tempfile( CLEANUP => 0 );
    print $fh "begin $workload\n";
    $self->{fh}       = $fh;
    $self->{filename} = $filename;
}

sub job : Job {
    my ( $self, $job, $workload ) = @_;
    my $fh = $self->{fh};
    print $fh "job $workload\n";
    return $self->{filename};
}

sub end {
    my ( $self, $job, $workload ) = @_;
    my $fh = $self->{fh};
    print $fh "end $workload\n";
    close $fh;
}

1;
