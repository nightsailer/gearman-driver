package Gearman::Driver::Console;

use Moose;
use POE qw(Component::Server::TCP);
use Try::Tiny;

=head1 NAME

Gearman::Driver::Console - Management console

=head1 SYNOPSIS

TODO: Add docs | telnet localhost 47300 etc

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
            my ( $command, @params ) = split /\s+/, $input;
            if ( $self->can($command) ) {
                try {
                    my @result = $self->$command(@params);
                    $heap->{client}->put($_) for @result;
                    $heap->{client}->put('.');
                }
                catch {
                    chomp($_);
                    $heap->{client}->put($_);
                };
            }
            else {
                $heap->{client}->put("ERR unknown_command: $command");
            }
        }
    );
}

=head1 COMMANDS

=head2 status

  name min_childs max_childs current_childs

=cut

sub status {
    my ($self) = @_;
    my @result = ();
    foreach my $job ( $self->driver->get_jobs ) {
        push @result, sprintf( "%s\t%d\t%d\t%d", $job->name, $job->min_childs, $job->max_childs, $job->count_childs );
    }
    return @result;
}

=head2 set_min_childs

  set_min_childs $job_name $min_childs_value

=cut

sub set_min_childs {
    my ( $self, $job_name, $min_childs ) = @_;

    if ( !defined($min_childs) or $min_childs !~ /^\d+$/ or $min_childs < 0 ) {
        die "ERR invalid_value: minchilds must be greater than 0\n";
    }

    my $job = $self->driver->get_job($job_name) || die "ERR invalid_job_name: $job_name\n";

    $job->min_childs($min_childs);

    return "OK";
}

=head2 set_max_childs

  set_max_childs $job_name $max_childs_value

=cut

sub set_max_childs {
    my ( $self, $job_name, $max_childs ) = @_;

    if ( !defined($max_childs) or $max_childs !~ /^\d+$/ or $max_childs < 0 ) {
        die "ERR invalid_value: max_childs must be >= 0\n";
    }

    my $job = $self->driver->get_job($job_name) || die "ERR invalid_job_name: $job_name\n";

    $job->min_childs($min_childs);

    return "OK";
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
