package Gearman::Driver::Console;

use Moose;
use POE qw(Component::Server::TCP);
use Try::Tiny;

=head1 NAME

Gearman::Driver::Console - Management console

=head1 SYNOPSIS

    $ ~/Gearman-Driver$ ./examples/driver.pl --console_port 12345 &
    [1] 32890
    $ ~/Gearman-Driver$ telnet localhost 12345
    Trying ::1...
    telnet: connect to address ::1: Connection refused
    Trying fe80::1...
    telnet: connect to address fe80::1: Connection refused
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.
    status
    GDExamples::Sleeper::ZzZzZzzz   3       6       3
    GDExamples::Sleeper::long_running_ZzZzZzzz      1       2       1
    GDExamples::WWW::is_online      0       1       0
    .

=head1 DESCRIPTION

By default L<Gearman::Driver> opens a management console which can
be used with a standard telnet client. It's possible to list all
running worker processes as well as changing min/max processes
on runtime.

Each successful L<command|/COMMANDS> ends with a dot. If a
command throws an error, a line starting with 'ERR' will be
returned.

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
            elsif ( $command eq 'quit' ) {
                delete $heap->{client};
            }
            else {
                $heap->{client}->put("ERR unknown_command: $command");
            }
        }
    );
}

=head1 COMMANDS

=head2 status

Parameters: C<none>

    GDExamples::Sleeper::ZzZzZzzz   3       6       3
    GDExamples::Sleeper::long_running_ZzZzZzzz      1       2       1
    GDExamples::WWW::is_online      0       1       0
    .

Columns are separated by tabs in this order:

=over 4

=item * job_name

=item * min_processes

=item * max_processes

=item * current_processes

=back

=cut

sub status {
    my ($self) = @_;
    my @result = ();
    foreach my $job ( $self->driver->get_jobs ) {
        push @result,
          sprintf( "%s\t%d\t%d\t%d", $job->name, $job->min_processes, $job->max_processes, $job->count_processes );
    }
    return @result;
}

=head2 set_min_processes

Parameters: C<job_name min_processes>

    set_min_processes asdf 5
    ERR invalid_job_name: asdf
    set_min_processes GDExamples::Sleeper::ZzZzZzzz ten
    ERR invalid_value: min_processes must be >= 0
    set_min_processes GDExamples::Sleeper::ZzZzZzzz 10
    ERR invalid_value: min_processes must be smaller than max_processes
    set_min_processes GDExamples::Sleeper::ZzZzZzzz 5
    OK
    .

=cut

*set_min_childs = \&set_min_processes;

sub set_min_processes {
    my ( $self, $job_name, $min_processes ) = @_;

    my $job = $self->_get_job($job_name);

    if ( !defined($min_processes) || $min_processes !~ /^\d+$/ || $min_processes < 0 ) {
        die "ERR invalid_value: min_processes must be >= 0\n";
    }

    if ( $min_processes > $job->max_processes ) {
        die "ERR invalid_value: min_processes must be smaller than max_processes\n";
    }

    $job->min_processes($min_processes);

    return "OK";
}

=head2 set_max_processes

Parameters: C<job_name max_processes>

    set_max_processes asdf 5
    ERR invalid_job_name: asdf
    set_max_processes GDExamples::Sleeper::ZzZzZzzz ten
    ERR invalid_value: max_processes must be >= 0
    set_max_processes GDExamples::Sleeper::ZzZzZzzz 0
    ERR invalid_value: max_processes must be greater than min_processes
    set_max_processes GDExamples::Sleeper::ZzZzZzzz 6
    OK
    .

=cut

*set_max_childs = \&set_max_processes;

sub set_max_processes {
    my ( $self, $job_name, $max_processes ) = @_;

    my $job = $self->_get_job($job_name);

    if ( !defined($max_processes) || $max_processes !~ /^\d+$/ || $max_processes < 0 ) {
        die "ERR invalid_value: max_processes must be >= 0\n";
    }

    if ( $max_processes < $job->min_processes ) {
        die "ERR invalid_value: max_processes must be greater than min_processes\n";
    }

    $job->max_processes($max_processes);

    return "OK";
}

=head2 quit

Parameters: C<none>

Closes your connection gracefully.

=head2 shutdown

Parameters: C<none>

Shuts L<Gearman::Driver> down.

=cut

sub shutdown {
    my ($self) = @_;
    $self->driver->shutdown;
}

sub _get_job {
    my ( $self, $job_name ) = @_;
    return $self->driver->get_job($job_name) || die "ERR invalid_job_name: $job_name\n";
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
