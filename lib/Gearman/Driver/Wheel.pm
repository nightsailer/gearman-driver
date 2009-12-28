package Gearman::Driver::Wheel;

use Moose;
use Gearman::XS::Worker;
use Gearman::XS qw(:constants);
use POE qw(Wheel::Run);

=head1 NAME

Gearman::Driver::Wheel - Handles the POE magic

=head1 SYNOPSIS

=head1 DESCRIPTION

Currently there's no public interface.

=cut

has 'driver' => (
    handles  => { log => 'log' },
    is       => 'rw',
    isa      => 'Gearman::Driver',
    required => 1,
    weak_ref => 1,
);

has 'name' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'method' => (
    is       => 'rw',
    isa      => 'Class::MOP::Method',
    required => 1,
);

has 'worker' => (
    is       => 'rw',
    isa      => 'Any',
    required => 1,
);

has 'server' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'max_childs' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

has 'min_childs' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

has 'childs' => (
    default => sub { [] },
    handles => {
        __add_child    => 'push',
        __remove_child => 'pop',
        all_childs     => 'sort',
        count_childs   => 'count',
    },
    is     => 'ro',
    isa    => 'ArrayRef[POE::Wheel::Run]',
    traits => [qw(Array)],
);

has 'gearman' => (
    is  => 'ro',
    isa => 'Gearman::XS::Worker',
);

has 'session' => (
    is  => 'ro',
    isa => 'POE::Session',
);

sub add_child {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'add_child' );
}

sub remove_child {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'remove_child' );
}

sub BUILD {
    my ($self) = @_;

    $self->{gearman} = Gearman::XS::Worker->new;
    $self->gearman->add_servers( $self->server );

    my $wrapper = sub {
        $self->method->body->( $self->worker, $self->driver, @_ );
    };

    my $ret = $self->gearman->add_function( $self->name, 0, $wrapper, '' );
    if ( $ret != GEARMAN_SUCCESS ) {
        die $self->gearman->error;
    }

    $self->{session} = POE::Session->create(
        object_states => [
            $self => {
                _start           => '_start',
                got_child_stdout => '_on_child_stdout',
                got_child_stderr => '_on_child_stderr',
                got_child_close  => '_on_child_close',
                got_child_signal => '_on_child_signal',
                got_sig_int      => '_on_sig_int',
                add_child        => '_add_child',
                remove_child     => '_remove_child',
            }
        ]
    );
}

sub _add_child {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

    my $child = POE::Wheel::Run->new(
        Program => sub {
            while (1) {
                my $ret = $self->gearman->work;
                if ( $ret != GEARMAN_SUCCESS ) {
                    die $self->gearman->error;
                }
            }
        },
        StdoutEvent => "got_child_stdout",
        StderrEvent => "got_child_stderr",
        CloseEvent  => "got_child_close",
        CloseOnCall => 1,
    );
    $kernel->sig_child( $child->PID, "got_child_signal" );

    # Wheel events include the wheel's ID.
    $heap->{children_by_wid}{ $child->ID } = $child;

    # Signal events include the process ID.
    $heap->{children_by_pid}{ $child->PID } = $child;

    $self->log->info( sprintf '(%d) [%s] Child started', $child->PID, $self->name );

    $self->__add_child($child);
}

sub _remove_child {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
    my $child = $self->__remove_child;
    delete $heap->{children_by_pid}{ $child->PID };
    $child->kill();
    $self->log->info( sprintf '(%d) [%s] Child killed', $child->PID, $self->name );
}

sub _start {
    $_[KERNEL]->sig( INT => 'got_sig_int' );
}

sub _on_child_stdout {
    my ( $self, $heap, $stdout, $wid ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];
    my $child = $heap->{children_by_wid}{$wid};
    $self->log->info( sprintf '(%d) [%s] STDOUT: %s', $child->PID, $self->name, $stdout );
}

sub _on_child_stderr {
    my ( $self, $heap, $stderr, $wid ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];
    my $child = $heap->{children_by_wid}{$wid};
    $self->log->info( sprintf '(%d) [%s] STDERR: %s', $child->PID, $self->name, $stderr );
}

sub _on_child_close {
    my ( $self, $heap, $wid ) = @_[ OBJECT, HEAP, ARG0 ];

    my $child = delete $heap->{children_by_wid}{$wid};

    # May have been reaped by on_child_signal().
    unless ( defined $child ) {
        $self->log->info( sprintf '[%s] Closed all pipes', $self->name );
        return;
    }

    $self->log->info( sprintf '(%d) [%s] Closed all pipes', $child->PID, $self->name );

    delete $heap->{children_by_pid}{ $child->PID };
}

sub _on_child_signal {
    my ( $self, $heap, $pid, $status ) = @_[ OBJECT, HEAP, ARG1 .. ARG2 ];

    my $child = delete $heap->{children_by_pid}{$pid};

    $self->log->info( sprintf '(%d) [%s] Exited with status %s', $pid, $self->name, $status );

    # May have been reaped by on_child_close().
    return unless defined $child;

    delete $heap->{children_by_wid}{ $child->ID };
}

sub _on_sig_int {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

    foreach my $pid ( keys %{ $heap->{children_by_pid} } ) {
        my $child = delete $heap->{children_by_pid}{$pid};
        $child->kill();
        $self->log->info( sprintf '(%d) [%s] Child killed', $pid, $self->name );
    }

    $kernel->sig_handled();

    exit(0);
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

=back

=cut

1;
