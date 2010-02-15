package    # hide from PAUSE
  Gearman::Driver::Test::Live::NS3::AddJob;

use base qw(Gearman::Driver::Test::Base::All);
use Moose;

use File::Temp qw(tempfile);

has 'filename' => ( is => 'ro' );
has 'fh'       => ( is => 'ro' );

has 'four_processes_done' => (
    default => 0,
    is      => 'rw',
    isa     => 'Bool',
);

sub begin {
    my ( $self, $job, $workload ) = @_;
    return unless $job->function_name eq 'Gearman::Driver::Test::Live::NS3::AddJob::begin_end';
    my ( $fh, $filename ) = tempfile( CLEANUP => 0 );
    print $fh "begin $workload\n";
    $self->{fh}       = $fh;
    $self->{filename} = $filename;
}

sub end {
    my ( $self, $job, $workload ) = @_;
    return unless $job->function_name eq 'Gearman::Driver::Test::Live::NS3::AddJob::begin_end';
    my $fh = $self->{fh};
    print $fh "end $workload\n";
    close $fh;
}

sub begin_end {
    my ( $self, $job, $workload ) = @_;
    my $fh = $self->{fh};
    print $fh "job $workload\n";
    return $self->{filename};
}

sub job1 {
    my ( $self, $job, $workload ) = @_;
    return $job->workload;
}

sub custom_encode {
    my ( $self, $result ) = @_;
    return "CUSTOMENCODE::${result}::CUSTOMENCODE";
}

sub custom_decode {
    my ( $self, $workload ) = @_;
    return "CUSTOMDECODE::${workload}::CUSTOMDECODE";
}

sub pid {
    return $$;
}

sub four_processes {
    my ( $self, $job, $workload ) = @_;
    if ( $self->four_processes_done ) {
        exit(1);
    }
    $self->four_processes_done(1);
    return $self->pid;
}

1;
