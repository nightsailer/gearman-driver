package    # hide from PAUSE
  Live::NS3::AddJob;

use Moose;
extends 'Gearman::Driver::Worker::Base';

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

sub end {
    my ( $self, $job, $workload ) = @_;
    my $fh = $self->{fh};
    print $fh "end $workload\n";
    close $fh;
}

sub job2 {
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

sub ten_childs {
    my ( $self, $job, $workload ) = @_;
    return $self->pid;
}

sub sleeper {
    my ( $self, $job, $workload ) = @_;
    my ( $sleep, $time ) = split /:/, $job->workload;
    sleep($sleep) if $sleep;
    return time - $time;
}

1;