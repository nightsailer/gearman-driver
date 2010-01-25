package Gearman::Driver::Console;

use Moose;
use POE qw(Component::Server::TCP);
use Module::Find;
use Moose::Util qw(apply_all_roles);
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

By default L<Gearman::Driver> provides a management console which can
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

    my @commands = findallmod Gearman::Driver::Console;
    apply_all_roles( $self => @commands );

    $self->{server} = POE::Component::Server::TCP->new(
        Alias       => "server",
        Port        => $self->port,
        ClientInput => sub {
            my ( $session, $heap, $input ) = @_[ SESSION, HEAP, ARG0 ];
            my ( $command, @params ) = split /\s+/, $input;
            $command ||= '';
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

All basic commands are implemented in
L<Gearman::Driver::Console::Basic>. It's very easy to extend this
console with new commands. Every module found in namespace
C<Gearman::Driver::Console::*> will be loaded. Each of those
modules has to be implemented as a L<Moose::Role>. You've got
access to two attributes/methods there:

=over 4

=item * C<driver> - reference to the L<Gearman::Driver> object

=item * C<server> - reference to the L<POE::Component::TCP::Server> object

=item * C<get_job($name)> - returns a L<Gearman::Driver::Job> object

=back

So a new command could look like:

    package Gearman::Driver::Console::List;

    use Moose::Role;

    sub list {
        my ($self) = @_;
        my @result = ();
        foreach my $job ( $self->driver->get_jobs ) {
            push @result, $job->name;
        }
        return @result;
    }

    1;

If you need to throw an error, just die and everything will work as
expected (as long as you do not forget the C<\n>):

    package Gearman::Driver::Console::Broken;

    use Moose::Role;

    sub broken {
        my ($self) = @_;
        die "ERR this is a broken command\n";
    }

    sub get_max_processes {
        my ( $self, $job_name ) = @_;
        my $job = $self->get_job($job_name); # this automatically dies if job is not found
        return $job->max_processes;
    }

    1;

Yes, that's all...

    $ ~/Gearman-Driver$ telnet localhost 47300
    Trying ::1...
    telnet: connect to address ::1: Connection refused
    Trying fe80::1...
    telnet: connect to address fe80::1: Connection refused
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.
    list
    GDExamples::Sleeper::ZzZzZzzz
    GDExamples::Sleeper::long_running_ZzZzZzzz
    GDExamples::WWW::is_online
    .
    broken
    ERR this is a broken command
    get_max_processes asdf
    ERR invalid_job_name: asdf
    get_max_processes GDExamples::Sleeper::ZzZzZzzz
    6
    .

=cut

sub get_job {
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

=item * L<Gearman::Driver::Console::Basic>

=item * L<Gearman::Driver::Job>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;
