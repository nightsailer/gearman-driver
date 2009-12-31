package Gearman::Driver;

use Moose;
use Moose::Util qw(apply_all_roles);
use Carp qw(croak);
use Gearman::Driver::Observer;
use Gearman::Driver::Job;
use Log::Log4perl qw(:easy);
use Module::Find;
use MooseX::Types::Path::Class;
use POE;
with qw(MooseX::Log::Log4perl MooseX::Getopt);

our $VERSION = '0.01003';

=head1 NAME

Gearman::Driver - Manages Gearman workers

=head1 SYNOPSIS

    package My::Workers::One;

    use base qw(Gearman::Driver::Worker);
    use Moose;

    # this method will be registered with gearmand as 'My::Workers::One::scale_image'
    sub scale_image : Job {
        my ( $self, $job, $workload ) = @_;
        # do something
    }

    # this method will be registered with gearmand as 'My::Workers::One::do_something_else'
    sub do_something_else : Job : MinChilds(2) : MaxChilds(15) {
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

Each module found in your namespace will be loaded and introspected,
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

=head2 namespaces

Will be passed to L<Module::Find> C<useall> method to load worker
modules. Each one of those modules has to be inherited from
L<Gearman::Driver::Worker> or a subclass of it. It's also possible
to use the full package name to load a single module/file. There is
also a method L<get_namespaces|Gearman::Driver/get_namespaces> which
returns a sorted list of all namespaces.

=over 4

=item * isa: C<ArrayRef>

=item * required: C<True>

=back

=cut

has 'namespaces' => (
    documentation => 'Example: --namespaces My::Workers --namespaces My::OtherWorkers',
    handles       => { get_namespaces => 'sort' },
    is            => 'rw',
    isa           => 'ArrayRef[Str]',
    required      => 1,
    traits        => [qw(Array)],
);

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

=head2 interval

Each n seconds L<Net::Telnet::Gearman> is used in
L<Gearman::Driver::Observer> to check status of free/running/busy
workers on gearmand. This is used to fork more workers depending
on the queue size and the MinChilds/MaxChilds
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

=item * default: C<[%d] %m%n>

=back

=cut

has 'loglayout' => (
    default       => '[%d] %m%n',
    documentation => 'Log message layout (default: [%d] %m%n)',
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

=head2 lib

This is just for convenience to extend C<@INC> from command line
using C<gearman_driver.pl>:

    gearman_driver.pl --lib ./lib --lib /custom/lib --namespaces My::Workers

=over 4

=item * isa: C<Str>

=back

=cut

has 'lib' => (
    default       => sub { [] },
    documentation => 'Example: --lib ./lib --lib /custom/lib',
    is            => 'rw',
    isa           => 'ArrayRef[Str]',
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
        'name'    => 'GDExamples::Sleeper::unknown_job',
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

=head2 modules

Every worker module loaded by L<Module::Find> will be added to this
list. There are also two methods:
L<get_modules|Gearman::Driver/get_modules> and
L<has_modules|Gearman::Driver/has_modules>.

=over 4

=item * isa: C<ArrayRef>

=item * readonly: C<True>

=back

=cut

has 'modules' => (
    default => sub { [] },
    handles => {
        _add_module => 'push',
        get_modules => 'sort',
        has_modules => 'count',
    },
    is     => 'ro',
    isa    => 'ArrayRef[Str]',
    traits => [qw(Array NoGetopt)],
);

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
        get_jobs => 'values',
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

has '+logger' => ( traits => [qw(NoGetopt)] );

=head1 METHODS

Every method except L<run|/run> might only be interesting for
people subclassing L<Gearman::Driver>.

=head2 run

This must be called after the L<Gearman::Driver> object is instantiated.

=cut

sub run {
    POE::Kernel->run();
}

=head2 get_namespaces

Returns a sorted list of L<namespaces|Gearman::Driver/namespaces>.

=head2 get_modules

Returns a sorted list of L<modules|Gearman::Driver/modules>.

=head2 has_modules

Returns the count of L<modules|Gearman::Driver/modules>.

=head2 has_job

Params: $name

Returns true/false if the job exists.

=head2 get_job

Params: $name

Returns the job instance.

=cut

sub BUILD {
    my ($self) = @_;
    push @INC, @{ $self->lib };
    $self->_setup_logger;
    $self->_load_namespaces;
    $self->_start_observer;
    $self->_start_session;

    # $self->_start_jobs;
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

sub _load_namespaces {
    my ($self) = @_;

    my @modules = ();
    foreach my $ns ( $self->get_namespaces ) {
        push @modules, useall $ns;
    }

    unless (@modules) {
        my $ns = join ', ', $self->get_namespaces;
        croak "Could not find any modules in those namespaces: $ns";
    }

    foreach my $module (@modules) {
        next unless $self->_is_valid_worker_subclass($module);
        next unless $self->_has_job_method($module);
        $self->_add_module($module);
    }

    unless ( $self->has_modules ) {
        my $modules = join ', ', @modules;
        croak "None of the modules have a method with 'Job' attribute set: $modules";
    }
}

sub _is_valid_worker_subclass {
    my ( $self, $module ) = @_;
    return 0 unless $module->can('meta');
    return 0 unless $module->meta->can('linearized_isa');
    return 0 unless grep $_ eq 'Gearman::Driver::Worker', $module->meta->linearized_isa;
    return 1;
}

sub _has_job_method {
    my ( $self, $module ) = @_;
    return 0 unless $module->meta->can('get_nearest_methods_with_attributes');
    foreach my $method ( $module->meta->get_nearest_methods_with_attributes ) {
        next unless grep $_ eq 'Job', @{ $method->attributes };
        return 1;
    }
    return 0;
}

sub _start_observer {
    my ($self) = @_;
    $self->{observer} = Gearman::Driver::Observer->new(
        callback => sub {
            my ($status) = @_;
            $self->_observer_callback($status);
        },
        interval => $self->interval,
        server   => $self->server,
    );
}

sub _observer_callback {
    my ( $self, $status ) = @_;
    foreach my $row (@$status) {
        if ( my $job = $self->get_job( $row->{name} ) ) {
            if ( $job->count_childs && $job->count_childs == $row->{busy} && $row->{queue} ) {
                my $diff = $row->{queue} - $row->{busy};
                my $free = $job->max_childs - $job->count_childs;
                if ($free) {
                    my $start = $diff > $free ? $free : $diff;
                    $job->add_child for 1 .. $start;
                }
            }
            elsif ( $job->count_childs && $job->count_childs > $job->min_childs && $row->{queue} == 0 ) {
                my $stop = $job->count_childs - $job->min_childs;
                $job->remove_child for 1 .. $stop;
            }
            elsif ( $job->count_childs < $job->min_childs ) {
                my $start = $job->min_childs - $job->count_childs;
                $job->add_child for 1 .. $start;
            }
        }
        else {
            $self->unknown_job_callback->( $self, $row ) if $row->{queue} > 0;
        }
    }
}

sub _start_session {
    my ($self) = @_;
    POE::Session->create(
        object_states => [
            $self => {
                _start  => '_start',
                got_sig => '_on_sig',
            }
        ]
    );
}

sub _on_sig {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

    foreach my $job ( $self->get_jobs ) {
        foreach my $child ( $job->get_childs ) {
            $self->log->info( sprintf '(%d) [%s] Child killed', $child->PID, $job->name );
            $child->kill();
        }
    }

    $kernel->sig_handled();

    exit(0);
}

sub _start {
    $_[KERNEL]->sig( $_ => 'got_sig' ) for qw(INT QUIT ABRT KILL TERM);
    $_[OBJECT]->_start_jobs;
}

sub _start_jobs {
    my ($self) = @_;

    foreach my $module ( $self->get_modules ) {
        my $worker = $module->new( server => $self->server );
        foreach my $method ( $module->meta->get_nearest_methods_with_attributes ) {
            apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );
            next unless $method->has_attribute('Job');
            my $name = $worker->prefix . $method->name;
            my $job  = Gearman::Driver::Job->new(
                driver     => $self,
                method     => $method,
                name       => $name,
                worker     => $worker,
                server     => $self->server,
                min_childs => $method->get_attribute('MinChilds'),
                max_childs => $method->get_attribute('MaxChilds'),
            );
            for ( 1 .. $method->get_attribute('MinChilds') ) {
                $job->add_child();
            }
            $self->_set_job( $name => $job );
        }
    }
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

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Job>

=item * L<Gearman::Driver::Worker>

=item * L<Gearman::XS>

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
