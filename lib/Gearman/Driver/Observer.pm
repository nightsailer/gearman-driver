package Gearman::Driver::Observer;

use Moose;
use Net::Telnet::Gearman;
use POE;

=head1 NAME

Gearman::Driver::Observer - Observes gearmand status interface

=head1 DESCRIPTION

Each n seconds L<Net::Telnet::Gearman> is used to fetch status of
free/running/busy workers from the Gearman server. L<Gearman::Driver>
decides to fork more workers depending on the queue size and the
MinChilds/MaxChilds attribute of the job methods.

Currently there's no public interface.

=cut

has 'callback' => (
    is       => 'rw',
    isa      => 'CodeRef',
    required => 1,
);

has 'interval' => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

has 'server' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'telnet' => (
    auto_deref => 1,
    default    => sub { [] },
    is         => 'ro',
    isa        => 'ArrayRef[Net::Telnet::Gearman]',
);

has 'session' => (
    is  => 'ro',
    isa => 'POE::Session',
);

sub BUILD {
    my ($self) = @_;

    foreach my $server ( split /,/, $self->server ) {
        my ( $host, $port ) = split /:/, $server;

        push @{ $self->{telnet} },
          Net::Telnet::Gearman->new(
            Host => $host || 'localhost',
            Port => $port || 4730,
          );
    }

    $self->{session} = POE::Session->create(
        object_states => [
            $self => {
                _start       => '_start',
                fetch_status => '_fetch_status'
            }
        ]
    );
}

sub _start {
    $_[KERNEL]->delay( fetch_status => $_[OBJECT]->interval );
}

sub _fetch_status {
    my %data = ();

    foreach my $telnet ( $_[OBJECT]->telnet ) {
        my $status = $telnet->status;

        foreach my $row (@$status) {
            $data{ $row->name } ||= {
                name    => $row->name,
                busy    => 0,
                free    => 0,
                queue   => 0,
                running => 0,
            };
            $data{ $row->name }{busy}    += $row->busy;
            $data{ $row->name }{free}    += $row->free;
            $data{ $row->name }{queue}   += $row->queue;
            $data{ $row->name }{running} += $row->running;
        }
    }

    $_[OBJECT]->callback->( [ values %data ] );

    $_[KERNEL]->delay( fetch_status => $_[OBJECT]->interval );
}

=head1 AUTHOR

Johannes Plunien E<lt>plu@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2009 by Johannes Plunien

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

=over 4

=item * L<Gearman::Driver>

=item * L<Gearman::Driver::Wheel>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;
