package Gearman::Driver::Job;

use Moose;
use Gearman::XS::Worker;
use Gearman::XS qw(:constants);
use POE qw(Wheel::Run);

=head1 NAME

Gearman::Driver::Job - Handles the POE magic

=head1 DESCRIPTION

This class is responsible for starting/stopping childs as well as
handling all pipes (STDOUT/STDERR/STDIN) of the childs. All events
are written to a logfile. Possible events are:

=over 4

=item * Starting childs

=item * STDOUT of childs

=item * STDERR of childs

=item * Stopping childs

=back

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
    default => sub { {} },
    handles => {
        count_childs => 'count',
        delete_child => 'delete',
        get_child    => 'get',
        get_childs   => 'values',
        get_pids     => 'keys',
        set_child    => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
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
        my ($job) = @_;

        my @args = ($job);

        if ( my $decoder = $self->method->get_attribute('Decode') ) {
            push @args, $self->worker->$decoder( $job->workload );
        }
        else {
            push @args, $job->workload;
        }

        $self->worker->begin(@args);

        my $result = eval { $self->method->body->( $self->worker, @args ) };
        my $error = $@;

        $self->worker->end(@args);

        die $error if $error;

        if ( my $encoder = $self->method->get_attribute('Encode') ) {
            $result = $self->worker->$encoder($result);
        }

        return $result;
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
                add_child        => '_add_child',
                remove_child     => '_remove_child',
            }
        ]
    );
}

sub _start {
    $_[KERNEL]->alias_set( $_[OBJECT]->name );
}

sub _add_child {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
    my $child = POE::Wheel::Run->new(
        Program => sub {
            if ( my $process_name = $self->worker->process_name( $0, $self->name ) ) {
                $0 = $process_name;
            }

            while (1) {
                my $ret = $self->gearman->work;
                if ( $ret != GEARMAN_SUCCESS ) {
                    $self->log->error( sprintf '[%s] Gearman error: %s', $self->name, $self->gearman->error );
                    exit(1);
                }
            }
        },
        StdoutEvent => "got_child_stdout",
        StderrEvent => "got_child_stderr",
        CloseEvent  => "got_child_close",
    );
    $kernel->sig_child( $child->PID, "got_child_signal" );

    # Wheel events include the wheel's ID.
    $heap->{wheels}{ $child->ID } = $child;

    $self->log->info( sprintf '(%d) [%s] Child started', $child->PID, $self->name );

    $self->set_child( $child->PID => $child );
}

sub _remove_child {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
    my ($pid) = ( $self->get_pids )[0];
    my $child = $self->delete_child($pid);
    $child->kill();
    $self->log->info( sprintf '(%d) [%s] Child killed', $child->PID, $self->name );
}

sub _on_child_stdout {
    my ( $self, $heap, $stdout, $wid ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];
    my $child = $heap->{wheels}{$wid};
    $self->log->info( sprintf '(%d) [%s] STDOUT: %s', $child->PID, $self->name, $stdout );
}

sub _on_child_stderr {
    my ( $self, $heap, $stderr, $wid ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];
    my $child = $heap->{wheels}{$wid};
    $self->log->info( sprintf '(%d) [%s] STDERR: %s', $child->PID, $self->name, $stderr );
}

sub _on_child_close {
    my ( $self, $heap, $wid ) = @_[ OBJECT, HEAP, ARG0 ];

    my $child = delete $heap->{wheels}{$wid};

    # May have been reaped by got_child_signal
    return unless defined $child;

    $self->delete_child( $child->PID );
}

sub _on_child_signal {
    my ( $self, $heap, $pid, $status ) = @_[ OBJECT, HEAP, ARG1 .. ARG2 ];

    my $child = $self->delete_child($pid);

    $self->log->info( sprintf '(%d) [%s] Exited with status %s', $pid, $self->name, $status );

    # May have been reaped by got_child_close
    return unless defined $child;

    delete $heap->{wheels}{ $child->ID };
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

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;
