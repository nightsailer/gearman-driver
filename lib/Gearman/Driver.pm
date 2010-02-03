package Gearman::Driver;

use Moose;
use Moose::Util qw(apply_all_roles);
use Class::MOP;
use Carp qw(croak);
use Gearman::Driver::Observer;
use Gearman::Driver::Console;
use Gearman::Driver::Job;
use Log::Log4perl qw(:easy);
use MooseX::Types::Path::Class;
use POE;
with qw(MooseX::Log::Log4perl MooseX::Getopt Gearman::Driver::Loader);

our $VERSION = '0.01022';

=head1 NAME

Gearman::Driver - Manages Gearman workers

=head1 SYNOPSIS

    package My::Workers::One;

    # Yes, you need to do it exactly this way
    use base qw(Gearman::Driver::Worker);
    use Moose;

    # this method will be registered with gearmand as 'My::Workers::One::scale_image'
    sub scale_image : Job {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    # this method will be registered with gearmand as 'My::Workers::One::do_something_else'
    sub do_something_else : Job : MinProcesses(2) : MaxProcesses(15) {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    # this method wont be registered with gearmand at all
    sub do_something_internal {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    1;

    package My::Workers::Two;

    use base qw(Gearman::Driver::Worker);
    use Moose;

    # this method will be registered with gearmand as 'My::Workers::Two::scale_image'
    sub scale_image : Job {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    1;

    package main;

    use Gearman::Driver;

    my $driver = Gearman::Driver->new(
        namespaces => [qw(My::Workers)],
        server     => 'localhost:4730,otherhost:4731',
        interval   => 60,
    );

    $driver->run;

=head1 DESCRIPTION

Having hundreds of Gearman workers running in separate processes can
consume a lot of RAM. Often many of these workers share the same
code/objects, like the database layer using L<DBIx::Class> for
example. This is where L<Gearman::Driver> comes in handy:

You write some base class which inherits from
L<Gearman::Driver::Worker>. Your base class loads your database layer
for example. Each of your worker classes inherit from that base
class. In the worker classes you can register single methods as jobs
with gearmand. It's even possible to control how many workers doing
that job/method in parallel. And this is the point where you'll
save some RAM: Instead of starting each worker in a separate process
L<Gearman::Driver> will fork each worker from the main process. This
will take advantage of copy-on-write on Linux and save some RAM.

There's only one mandatory parameter which has to be set when calling
the constructor: namespaces

    use Gearman::Driver;
    my $driver = Gearman::Driver->new( namespaces => [qw(My::Workers)] );

See also: L<namespaces|/namespaces>. If you do not set
L<server|/server> (gearmand) attribute the default will be used:
C<localhost:4730>

Each module found in your namespaces will be loaded and introspected,
looking for methods having the 'Job' attribute set:

    package My::Workers::ONE;

    sub scale_image : Job {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

This method will be registered as job function with gearmand, verify
it by doing:

    plu@mbp ~$ telnet localhost 4730
    Trying ::1...
    Connected to localhost.
    Escape character is '^]'.
    status
    My::Workers::ONE::scale_image   0       0       1
    .
    ^]
    telnet> Connection closed.

If you dont like to use the full package name you can also specify
a custom prefix:

    package My::Workers::ONE;

    sub prefix { 'foo_bar_' }

    sub scale_image : Job {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

This would register 'foo_bar_scale_image' with gearmand.

See also: L<prefix|Gearman::Driver::Worker/prefix>

=head1 ATTRIBUTES

See also L<Gearman::Driver::Loader/ATTRIBUTES>.

=head2 server

A list of Gearman servers the workers should connect to. The format
for the server list is: C<host[:port][,host[:port]]>

See also: L<Gearman::XS>

=over 4

=item * default: C<localhost:4730>

=item * isa: C<Str>

=back

=cut

has 'server' => (
    default       => 'localhost:4730',
    documentation => 'Gearman host[:port][,host[:port]]',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 console_port

Gearman::Driver has a telnet management console, see also:

L<Gearman::Driver::Console>

=over 4

=item * default: C<47300>

=item * isa: C<Int>

=back

Set this to C<0> to disable management console at all.

=cut

has 'console_port' => (
    default       => 47300,
    documentation => 'Port of management console (default: 47300)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 interval

Each n seconds L<Net::Telnet::Gearman> is used in
L<Gearman::Driver::Observer> to check status of free/running/busy
workers on gearmand. This is used to fork more workers depending
on the queue size and the MinProcesses/MaxProcesses
L<attribute|Gearman::Driver::Worker/METHODATTRIBUTES> of the
job method. See also: L<Gearman::Driver::Worker>

=over 4

=item * default: C<5>

=item * isa: C<Int>

=back

=cut

has 'interval' => (
    default       => '5',
    documentation => 'Interval in seconds (see Gearman::Driver::Observer)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 max_idle_time

Whenever L<Gearman::Driver::Observer> notices that there are more
processes running than actually necessary (depending on min_processes
and max_processes setting) it will kill them. By default this happens
immediately. If you change this value to C<300>, a process which is
not necessary is killed after 300 seconds.

Please remember that this also depends on what value you set
L</interval> to. The max_idle_time is only checked each n seconds
where n is L</interval>. Besides that it makes only sense when you
have workers where L<Gearman::Driver::Worker/MinProcesses> is set to
C<0>.

=over 4

=item * default: C<0>

=item * isa: C<Int>

=back

=cut

has 'max_idle_time' => (
    default       => '0',
    documentation => 'How many seconds a worker may be idle before its killed',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 logfile

Path to logfile.

=over 4

=item * isa: C<Str>

=item * default: C<gearman_driver.log>

=back

=cut

has 'logfile' => (
    coerce        => 1,
    default       => 'gearman_driver.log',
    documentation => 'Path to logfile (default: gearman_driver.log)',
    is            => 'rw',
    isa           => 'Path::Class::File',
);

=head2 loglayout

See also L<Log::Log4perl>.

=over 4

=item * isa: C<Str>

=item * default: C<[%d] %p %m%n>

=back

=cut

has 'loglayout' => (
    default       => '[%d] %p %m%n',
    documentation => 'Log message layout (default: [%d] %p %m%n)',
    is            => 'rw',
    isa           => 'Str',
);

=head2 loglevel

See also L<Log::Log4perl>.

=over 4

=item * isa: C<Str>

=item * default: C<INFO>

=back

=cut

has 'loglevel' => (
    default       => 'INFO',
    documentation => 'Log level (default: INFO)',
    is            => 'rw',
    isa           => 'Str',
);

=head2 unknown_job_callback

Whenever L<Gearman::Driver::Observer> sees a job that isnt handled
it will call this CodeRef, passing following arguments:

=over 4

=item * C<$driver>

=item * C<$status>

=back

    my $driver = Gearman::Driver->new(
        namespaces           => [qw(My::Workers)],
        unknown_job_callback => sub {
            my ( $driver, $status ) = @_;
            # notify nagios here for example
        }
    );

C<$status> might look like:

    $VAR1 = {
        'busy'    => 0,
        'free'    => 0,
        'name'    => 'GDExamples::Convert::unknown_job',
        'queue'   => 6,
        'running' => 0
    };

=cut

has 'unknown_job_callback' => (
    default => sub {
        sub { }
    },
    is     => 'rw',
    isa    => 'CodeRef',
    traits => [qw(NoGetopt)],
);

=head1 INTERNAL ATTRIBUTES

This might be interesting for subclassing L<Gearman::Driver>.

=head2 jobs

Stores all L<Gearman::Driver::Job> instances. The key is the name
the job gets registered with gearmand. There are also two methods:
L<get_job|Gearman::Driver/get_job> and
L<has_job|Gearman::Driver/has_job>.

Example:

    {
        'My::Workers::ONE::scale_image'       => bless( {...}, 'Gearman::Driver::Job' ),
        'My::Workers::ONE::do_something_else' => bless( {...}, 'Gearman::Driver::Job' ),
        'My::Workers::TWO::scale_image'       => bless( {...}, 'Gearman::Driver::Job' ),
    }

=over 4

=item * isa: C<HashRef>

=item * readonly: C<True>

=back

=cut

has 'jobs' => (
    default => sub { {} },
    handles => {
        _set_job => 'set',
        get_job  => 'get',
        has_job  => 'defined',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash NoGetopt)],
);

=head2 observer

Instance of L<Gearman::Driver::Observer>.

=over 4

=item * isa: C<Gearman::Driver::Observer>

=item * readonly: C<True>

=back

=cut

has 'observer' => (
    is     => 'ro',
    isa    => 'Gearman::Driver::Observer',
    traits => [qw(NoGetopt)],
);

=head2 console

Instance of L<Gearman::Driver::Console>.

=over 4

=item * isa: C<Gearman::Driver::Console>

=item * readonly: C<True>

=back

=cut

has 'console' => (
    is     => 'ro',
    isa    => 'Gearman::Driver::Console',
    traits => [qw(NoGetopt)],
);

=head2 extended_status

Enables/disables extended status information in
L<management console|Gearman::Driver::Console::Basic/status> like
lastrun, lasterror and lasterror_msg.

=over 4

=item * isa: C<Bool>

=item * default: C<1>

=back

=cut

has 'extended_status' => (
    default       => 1,
    documentation => 'Show extended status infos in management console',
    is            => 'rw',
    isa           => 'Bool',
);

has 'session' => (
    is     => 'ro',
    isa    => 'POE::Session',
    traits => [qw(NoGetopt)],
);

# child communication socket
has 'cc_socket' => (
    default => "/tmp/gearman_driver-$$.sock",
    is      => 'ro',
    isa     => 'Str',
    traits  => [qw(NoGetopt)],
);

has 'pid' => (
    default => $$,
    is      => 'ro',
    isa     => 'Int',
);

has '+logger'  => ( traits => [qw(NoGetopt)] );
has '+wanted'  => ( traits => [qw(NoGetopt)] );
has '+modules' => ( traits => [qw(NoGetopt)] );

=head1 METHODS

=head2 add_job

There's one mandatory param (hashref) with following keys:

=over 4

=item * decode (optionally)

Name of a decoder method in your worker object.

=item * encode (optionally)

Name of a encoder method in your worker object.

=item * method (mandatory)

Reference to a CodeRef which will get invoked.

=item * min_processes (mandatory)

Minimum number of processes that should be forked.

=item * max_processes (mandatory)

Maximum number of processes that may be forked.

=item * name (mandatory)

Job name/alias that method should be registered with Gearman.

=item * object (mandatory)

Object that should be passed as first parameter to the job method.

=back

Basically you never really need this method if you use
L</namespaces>. But L</namespaces> depend on method attributes which
some people do hate. In this case, feel free to setup your C<$driver>
this way:

    package My::Workers::One;

    use Moose;
    use JSON::XS;
    extends 'Gearman::Driver::Worker::Base';

    sub scale_image {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    # this method will be registered with gearmand as 'My::Workers::One::do_something_else'
    sub do_something_else {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    sub encode_json {
        my ( $self, $result ) = @_;
        return JSON::XS::encode_json($result);
    }

    sub decode_json {
        my ( $self, $workload ) = @_;
        return JSON::XS::decode_json($workload);
    }

    1;

    package main;

    use Gearman::Driver;
    use My::Workers::One;

    my $driver = Gearman::Driver->new(
        server   => 'localhost:4730,otherhost:4731',
        interval => 60,
    );

    my $worker = My::Workers::One->new();

    foreach my $method (qw(scale_image do_something_else)) {
        $driver->add_job(
            decode        => 'decode_json',
            encode        => 'encode_json',
            max_processes => 5,
            method        => $worker->meta->find_method_by_name($method)->body,
            min_processes => 1,
            name          => $method,
            object        => $worker,
        );
    }

    $driver->run;


=cut

sub add_job {
    my ( $self, $params ) = @_;

    $params->{max_processes} = delete $params->{max_childs} if defined $params->{max_childs};
    $params->{min_processes} = delete $params->{min_childs} if defined $params->{min_childs};

    my $job = Gearman::Driver::Job->new(
        driver        => $self,
        decode        => $params->{decode} || '',
        encode        => $params->{encode} || '',
        max_processes => $params->{max_processes},
        method        => $params->{method},
        min_processes => $params->{min_processes},
        name          => $params->{name},
        server        => $self->server,
        worker        => $params->{object},
    );

    $self->_set_job( $params->{name} => $job );

    $self->log->debug( sprintf "Added new job: %s (processes: %d)", $params->{name}, $params->{min_processes} );

    return 1;
}

=head2 get_jobs

Returns all L<Gearman::Driver::Job> objects ordered by jobname.

=cut

sub get_jobs {
    my ($self) = @_;
    my @result = ();
    foreach my $name ( sort keys %{ $self->jobs } ) {
        push @result, $self->get_job($name);
    }
    return @result;
}

=head2 run

This must be called after the L<Gearman::Driver> object is instantiated.

=cut

sub run {
    my ($self) = @_;
    push @INC, @{ $self->lib };
    $self->load_namespaces;
    $self->_start_observer;
    $self->_start_console;
    $self->_start_session;
    POE::Kernel->run();
}

=head2 shutdown

Sends TERM signal to all child processes and exits Gearman::Driver.

=cut

sub shutdown {
    my ($self) = @_;
    POE::Kernel->signal( $self->{session}, 'TERM' );
}

sub DEMOLISH {
    my ($self) = @_;
    if ( $self->pid eq $$ ) {
        $self->shutdown;
        unlink $self->cc_socket;
    }
}

=head2 has_job

Params: $name

Returns true/false if the job exists.

=head2 get_job

Params: $name

Returns the job instance.

=cut

sub BUILD {
    my ($self) = @_;
    $self->_setup_logger;
}

sub _setup_logger {
    my ($self) = @_;

    Log::Log4perl->easy_init(
        {
            file   => sprintf( '>>%s', $self->logfile ),
            layout => $self->loglayout,
            level  => $self->loglevel,
        },
    );
}

sub _start_observer {
    my ($self) = @_;
    if ( $self->interval > 0 ) {
        $self->{observer} = Gearman::Driver::Observer->new(
            callback => sub {
                my ($status) = @_;
                $self->_observer_callback($status);
            },
            interval => $self->interval,
            server   => $self->server,
        );
    }
}

sub _start_console {
    my ($self) = @_;
    if ( $self->console_port > 0 ) {
        $self->{console} = Gearman::Driver::Console->new(
            driver => $self,
            port   => $self->console_port,
        );
    }
}

sub _observer_callback {
    my ( $self, $status ) = @_;
    foreach my $row (@$status) {
        if ( my $job = $self->get_job( $row->{name} ) ) {
            if ( $job->count_processes <= $row->{busy} && $row->{queue} ) {
                my $diff = $row->{queue} - $row->{busy};
                my $free = $job->max_processes - $job->count_processes;
                if ($free) {
                    my $start = $diff > $free ? $free : $diff;
                    $self->log->debug( sprintf "Starting %d new process(es) of type %s", $start, $job->name );
                    $job->add_process for 1 .. $start;
                }
            }

            elsif ( $job->count_processes && $job->count_processes > $job->min_processes && $row->{queue} == 0 ) {
                my $idle = time - $self->console->get_lastrun( $job->name );
                if ( $idle >= $self->max_idle_time ) {
                    my $stop = $job->count_processes - $job->min_processes;
                    $self->log->debug( sprintf "Stopping %d process(es) of type %s (idle: %d)",
                        $stop, $job->name, $idle );
                    $job->remove_process for 1 .. $stop;
                }
            }
        }
        else {
            $self->unknown_job_callback->( $self, $row ) if $row->{queue} > 0;
        }
    }
}

sub _start_session {
    my ($self) = @_;
    $self->{session} = POE::Session->create(
        object_states => [
            $self => {
                _start            => '_start',
                got_sig           => '_on_sig',
                monitor_processes => '_monitor_processes',
            }
        ]
    );
}

sub _on_sig {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

    foreach my $job ( $self->get_jobs ) {
        foreach my $process ( $job->get_processes ) {
            $self->log->info( sprintf '(%d) [%s] Process killed', $process->PID, $job->name );
            $process->kill();
        }
    }

    $kernel->sig_handled();

    exit(0);
}

sub _start {
    $_[KERNEL]->sig( $_ => 'got_sig' ) for qw(INT QUIT ABRT KILL TERM);
    $_[OBJECT]->_add_jobs;
    $_[OBJECT]->_start_jobs;
    $_[KERNEL]->delay( monitor_processes => 5 );
}

sub _add_jobs {
    my ($self) = @_;

    foreach my $module ( $self->get_modules ) {
        my $worker = $module->new( server => $self->server );

        foreach my $method ( $module->meta->get_nearest_methods_with_attributes ) {
            apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );

            $method->default_attributes( $worker->default_attributes );
            $method->override_attributes( $worker->override_attributes );

            next unless $method->has_attribute('Job');

            $self->add_job(
                {
                    decode        => $method->get_attribute('Decode'),
                    encode        => $method->get_attribute('Encode'),
                    max_processes => $method->get_attribute('MaxProcesses'),
                    method        => $method->body,
                    min_processes => $method->get_attribute('MinProcesses'),
                    name          => $worker->prefix . $method->name,
                    object        => $worker,
                }
            );
        }

    }
}

sub _start_jobs {
    my ($self) = @_;

    foreach my $job ( $self->get_jobs ) {
        for ( 1 .. $job->min_processes ) {
            $job->add_process();
        }
    }
}

sub _monitor_processes {
    my $self = $_[OBJECT];
    foreach my $job ( $self->get_jobs ) {
        if ( $job->count_processes < $job->min_processes ) {
            my $start = $job->min_processes - $job->count_processes;
            $self->log->debug( sprintf "Starting %d new process(es) of type %s", $start, $job->name );
            $job->add_process for 1 .. $start;
        }
    }
    $_[KERNEL]->delay( monitor_processes => 5 );
}

no Moose;

__PACKAGE__->meta->make_immutable;

=head1 SCRIPT

There's also a script C<gearman_driver.pl> which is installed with
this distribution. It just instantiates L<Gearman::Driver> with its
default values, having most of the options exposed to the command
line using L<MooseX::Getopt>.

    usage: gearman_driver.pl [long options...]
            --loglevel          Log level (default: INFO)
            --lib               Example: --lib ./lib --lib /custom/lib
            --server            Gearman host[:port][,host[:port]]
            --logfile           Path to logfile (default: gearman_driver.log)
            --console_port      Port of management console (default: 47300)
            --interval          Interval in seconds (see Gearman::Driver::Observer)
            --loglayout         Log message layout (default: [%d] %p %m%n)
            --namespaces        Example: --namespaces My::Workers --namespaces My::OtherWorkers

=head1 AUTHOR

Johannes Plunien E<lt>plu@cpan.orgE<gt>

=head1 CONTRIBUTORS

Uwe Voelker, <uwe.voelker@gmx.de>

=head1 COPYRIGHT AND LICENSE

Copyright 2009 by Johannes Plunien

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

=over 4

=item * L<Gearman::Driver::Adaptor>

=item * L<Gearman::Driver::Console>

=item * L<Gearman::Driver::Console::Basic>

=item * L<Gearman::Driver::Console::Client>

=item * L<Gearman::Driver::Job>

=item * L<Gearman::Driver::Loader>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=item * L<Gearman::XS>

=item * L<Gearman>

=item * L<Gearman::Server>

=item * L<Log::Log4perl>

=item * L<Module::Find>

=item * L<Moose>

=item * L<MooseX::Getopt>

=item * L<MooseX::Log::Log4perl>

=item * L<MooseX::MethodAttributes>

=item * L<Net::Telnet::Gearman>

=item * L<POE>

=item * L<http://www.gearman.org/>

=back

=head1 REPOSITORY

L<http://github.com/plu/gearman-driver/>

=cut

1;
