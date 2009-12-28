package Gearman::Driver;

use Moose;
use Carp qw(croak);
use Gearman::Driver::Observer;
use Gearman::Driver::Wheel;
use Log::Log4perl qw(:easy);
use Module::Find;
use MooseX::Types::Path::Class;
with qw(MooseX::Log::Log4perl MooseX::Getopt);

our $VERSION = '0.01000';

=head1 NAME

Gearman::Driver - Manage Gearman workers

=head1 SYNOPSIS

    package My::Workers::One;

    use base qw(Gearman::Driver::Worker);
    use Moose;

    # this method will be registered at gearmand as 'My::Workers::One::scale_image'
    sub scale_image : Job {}

    # this method will be registered at gearmand as 'My::Workers::One::do_something_else'
    sub do_something_else : Job : MinProcs(2) : MaxProcs(15) {}

    # this method wont be registered at gearmand at all
    sub do_something_internal {}

    package My::Workers::Two;

    use base qw(Gearman::Driver::Worker);
    use Moose;

    # this method will be registered at gearmand as 'My::Workers::Two::scale_image'
    sub scale_image : Job {}

    package main;

    use Gearman::Driver;

    my $driver = Gearman::Driver->new_with_options(
        namespaces => [qw(My::Workers)],
        server     => 'localhost:4730,otherhost:4731',
        interval   => 60,
    );

    $driver->run;

=head1 DESCRIPTION

=head1 ATTRIBUTES

=head2 namespaces

Will be passed to L<Module::Find> C<useall> method to load worker
modules. Each one of those modules has to be inherited from
L<Gearman::Driver::Worker> or a subclass of it. It's also possible
to use the full module name to load a single module.

=over 4

=item * isa: ArrayRef

=item * required: True

=back

=cut

has 'namespaces' => (
    handles => {
        all_namespaces => 'sort',
        has_namespaces => 'count',
    },
    is       => 'rw',
    isa      => 'ArrayRef[Str]',
    required => 1,
    traits   => [qw(Array)],
);

=head2 modules

Every worker module loaded by L<Module::Find> will be added to this
list.

=over 4

=item * isa: ArrayRef

=item * readonly: True

=back

=cut

has 'modules' => (
    default => sub { [] },
    handles => {
        add_module  => 'push',
        all_modules => 'sort',
        has_modules => 'count',
    },
    is     => 'ro',
    isa    => 'ArrayRef[Str]',
    traits => [qw(Array NoGetopt)],
);

=head2 wheels

Stores all L<Gearman::Driver::Wheel> instances. The key is the name
the job gets registered on Gearman.

=over 4

=item * isa: HashRef

=item * readonly: True

=back

=cut

has 'wheels' => (
    default => sub { {} },
    handles => {
        set_wheel => 'set',
        get_wheel => 'get',
        has_wheel => 'defined',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash NoGetopt)],
);

=head2 server

A list of Gearman servers the workers should connect to. The format
for the server list is: C<host[:port][,host[:port]]>

=over 4

=item * default: localhost:4730

=item * isa: Str

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
workers on Gearman. This is used to fork more workers depending
on the queue size and the MinProcs/MaxProcs attribute of the
job method. See also: L<Gearman::Driver::Worker>

=over 4

=item * default: 5

=item * isa: Int

=back

=cut

has 'interval' => (
    default       => '5',
    documentation => 'Interval in seconds (see Gearman::Driver::Observer)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 observer

Instance of L<Gearman::Driver::Observer>.

=over 4

=item * isa: Gearman::Driver::Observer

=item * readonly: True

=back

=cut

has 'observer' => (
    is     => 'ro',
    isa    => 'Gearman::Driver::Observer',
    traits => [qw(NoGetopt)],
);

=head2 logfile

=over 4

=item * isa: Str

=item * default: gearman_driver.log

=cut

has 'logfile' => (
    coerce        => 1,
    default       => 'gearman_driver.log',
    documentation => 'Path to logfile (default: gearman_driver.log)',
    is            => 'ro',
    isa           => 'Path::Class::File',
);

has '+logger' => ( traits => [qw(NoGetopt)] );

=head1 METHODS

=head2 run

This must be called after the L<Gearman::Driver> object is instantiated.

=cut

sub run {
    POE::Kernel->run();
}

sub BUILD {
    my ($self) = @_;

    Log::Log4perl->easy_init(
        {
            level  => $DEBUG,
            file   => sprintf( '>>%s', $self->logfile ),
            layout => '[%d] [%F{1}:%L] %m%n',
        },
    );

    $self->_load_namespaces;
    $self->_start_observer;
    $self->_start_wheels;
}

sub _load_namespaces {
    my ($self) = @_;

    my @modules = ();
    foreach my $ns ( $self->all_namespaces ) {
        push @modules, useall $ns;
    }

    unless (@modules) {
        my $ns = join ', ', $self->all_namespaces;
        croak "Could not find any modules in those namespaces: $ns";
    }

    foreach my $module (@modules) {
        next unless $self->_is_valid_worker_subclass($module);
        next unless $self->_has_job_method($module);
        $self->add_module($module);
    }

    unless ( $self->has_modules ) {
        my $modules = join ', ', @modules;
        croak "None of the modules have a method with 'Job' attribute set: $modules";
    }
}

sub _is_valid_worker_subclass {
    my ( $self, $module ) = @_;
    return 0 unless $module->can('meta');
    return 0 unless grep $_ eq 'Gearman::Driver::Worker', $module->meta->superclasses;
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
        if ( $self->has_wheel( $row->{name} ) ) {
            warn "TODO: IMPLEMENT STATUS CALLBACK ... " . $row->{name};

            # E.g. if (queue_too_full) { $wheel->add_child() }
        }
        else {
            warn "UNKNOWN JOB: " . $row->{name};
        }
    }
}

sub _start_wheels {
    my ($self) = @_;

    foreach my $module ( $self->all_modules ) {
        my $worker = $module->new();
        foreach my $method ( $module->meta->get_nearest_methods_with_attributes ) {
            my $attr  = $worker->_parse_attributes( $method->attributes );
            my $name  = $worker->prefix . $method->name;
            my $wheel = Gearman::Driver::Wheel->new(
                method => $method,
                name   => $name,
                worker => $worker,
                server => $self->server,
            );
            for ( 1 .. $attr->{MinProcs} ) {
                $wheel->add_child();
            }
            $self->set_wheel( $name => $wheel );
        }
    }
}

=head1 AUTHOR

Johannes Plunien E<lt>plu@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2009 by Johannes Plunien

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

=over 4

=item * L<Gearman::XS>

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
