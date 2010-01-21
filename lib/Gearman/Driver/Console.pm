package Gearman::Driver::Console;

use Moose;
use POE qw(Component::Server::TCP);

=head1 NAME

Gearman::Driver::Console - Management console

=head1 DESCRIPTION

TODO: Add docs

=cut

has 'port' => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

has 'server' => (
    is  => 'ro',
    isa => 'POE::Component::Server::TCP',
);

has 'driver' => (
    handles  => { log => 'log' },
    is       => 'rw',
    isa      => 'Gearman::Driver',
    required => 1,
    weak_ref => 1,
);

sub BUILD {
    my ($self) = @_;

    $self->{server} = POE::Component::Server::TCP->new(
        Alias       => "server",
        Port        => $self->port,
        ClientInput => sub {
            my ( $session, $heap, $input ) = @_[ SESSION, HEAP, ARG0 ];
            print "Session ", $session->ID(), " got input: $input\n";
            $heap->{client}->put($input);
        }
    );
}

no Moose;

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

Johannes Plunien E<lt>plu@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2009 by Johannes Plunien

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

=over 4

=item * L<Gearman::Driver>

=item * L<Gearman::Driver::Job>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;
