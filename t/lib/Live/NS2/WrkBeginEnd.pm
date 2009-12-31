package    # hide from PAUSE
  Live::NS2::WrkBeginEnd;

use base qw(Gearman::Driver::Worker);
use Moose;
use File::Temp qw(tempfile);

sub begin {
    my ( $self, $job, $workload ) = @_;
    open my $fh, ">>$workload" or die "cannot open file $workload: $!";
    print $fh "begin ...\n";
    close $fh;
}

sub job : Job {
    my ( $self, $job, $workload ) = @_;
    die;
}

sub end {
    my ( $self, $job, $workload ) = @_;
    open my $fh, ">>$workload" or die "cannot open file $workload: $!";
    print $fh "end ...\n";
    close $fh;
}

1;
