package    # hide from PAUSE
  TestLib;

use strict;
use warnings;
use FindBin qw( $Bin );
use Gearman::Driver;
use Net::Telnet;

my ( $host, $port ) = ( '127.0.0.1', 4731 );

$|++;

BEGIN {
    eval "require Gearman::XS";
    unless ($@) {
        eval "require Gearman::XS qw(:constants);";
        eval "require Gearman::XS::Client;";
        eval "require Gearman::XS::Server;";
    }
    else {
        eval "require Gearman::Client;";
        eval "require Gearman::Server;";
        eval "require Danga::Socket;";
    }
}

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
}

sub gearman_client {
    my ( $self, $h, $p ) = @_;
    $h ||= $host;
    $p ||= $port;

    my $client;
    if ( has_xs() ) {
        $client = Gearman::XS::Client->new();
        $client->add_server( $h, $p );
    }
    else {
        no strict 'refs';
        $client = Gearman::Client->new( exceptions => 1 );
        $client->job_servers("${h}:${p}");

        # Fake Gearman::XS interface
        *{"Gearman::Client::do"} = sub { my $result = shift->do_task(@_); return ( 0, $result ? $$result : 0 ); };
        *{"Gearman::Client::do_background"} = sub { shift->dispatch_background(@_); };
    }
    return $client;
}

sub gearman_server_run {
    if ( has_xs() ) {
        my $server = Gearman::XS::Server->new( $host, $port );
        $server->run();
    }
    else {
        my $server = Gearman::Server->new( port => $port );
        Danga::Socket->EventLoop();
    }
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

sub has_xs {
    eval "require Gearman::XS";
    return 1 unless $@;
    return 0;
}

sub DESTROY {
    my ($self) = @_;

    foreach my $proc (qw/gearmand_pid gearman_driver_pid/) {
        system 'kill', $self->{$proc} if $self->{$proc};
    }
}

1;
