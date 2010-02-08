package    # hide from PAUSE
  TestLib;

use strict;
use warnings;
use FindBin qw( $Bin );
use Gearman::Driver;
use Net::Telnet;
use Gearman::Client;
use Gearman::Server;
use Danga::Socket;

my ( $host, $port ) = ( '127.0.0.1', 4731 );

$|++;

sub new {
    return bless {}, shift;
}

sub run_gearmand {
    my ($self) = @_;

    unless ( $self->{gearmand_pid} = fork ) {
        die "cannot fork: $!" unless defined $self->{gearmand_pid};

        my @cmd = ( $^X, "$Bin/gearmand.pl" );

        exec @cmd or die "Could not exec $Bin/gearmand.pl";

        exit(0);
    }

    sleep(5);
    warn sprintf "gearmand started (%d)\n", $self->{gearmand_pid};
}

sub run_gearman_driver {
    my ($self) = @_;

    unless ( $self->{gearman_driver_pid} = fork ) {
        die "cannot fork: $!" unless defined $self->{gearman_driver_pid};

        my @cmd = ( $^X, "$Bin/gearman_driver.pl" );

        exec @cmd or die "Could not exec $Bin/gearman_driver.pl";

        exit(0);
    }

    sleep(5);
    warn sprintf "gearman-driver started (%d)\n", $self->{gearman_driver_pid};
}

sub gearman_client {
    my ( $self, $h, $p ) = @_;

    $h ||= $host;
    $p ||= $port;

    my $client = Gearman::Client->new( exceptions => 1 );
    $client->job_servers("${h}:${p}");

    return $client;
}

sub gearman_server_run {
    my $server = Gearman::Server->new( port => $port );
    Danga::Socket->EventLoop();
}

sub gearman_driver {
    return Gearman::Driver->new(
        max_idle_time => 5,
        interval      => 1,
        loglevel      => 'DEBUG',
        namespaces    => [qw(Live)],
        server        => join( ':', $host, $port ),
    );
}

sub telnet_client {
    my $telnet = Net::Telnet->new(
        Timeout => 5,
        Host    => '127.0.0.1',
        Port    => 47300,
    );
    $telnet->open;
    return $telnet;
}

sub DESTROY {
    my ($self) = @_;

    foreach my $proc (qw/gearmand_pid gearman_driver_pid/) {
        system 'kill', $self->{$proc} if $self->{$proc};
    }
}

1;
