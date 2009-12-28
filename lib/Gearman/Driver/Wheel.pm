package Gearman::Driver::Wheel;

use Moose;
use Gearman::XS::Worker;
use Gearman::XS qw(:constants);
use POE qw(Wheel::Run);

=head1 NAME

Gearman::Driver::Wheel - TBD

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 ATTRIBUTES

=head2 method

=cut

has 'method' => (
    is       => 'rw',
    isa      => 'Class::MOP::Method',
    required => 1,
);

=head2 name

=cut

has 'name' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

=head2 worker

=cut

has 'worker' => (
    is       => 'rw',
    isa      => 'Any',
    required => 1,
);

=head2 server

=cut

has 'server' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

=head2 childs

=cut

has 'childs' => (
    is  => 'ro',
    isa => 'ArrayRef[POE::Wheel::Run]',
);

=head2 gearman

=cut

has 'gearman' => (
    is  => 'ro',
    isa => 'Gearman::XS::Worker',
);

=head2 session

=cut

has 'session' => (
    is  => 'ro',
    isa => 'POE::Session',
);

=head1 METHODS

=head2 add_child

=cut

sub add_child {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'add_child' );
}

sub BUILD {
    my ($self) = @_;

    $self->{gearman} = Gearman::XS::Worker->new;
    $self->gearman->add_servers( $self->server );

    my $wrapper = sub {
        $self->method->body->( $self->worker, @_ );
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

    print( "Child pid ", $child->PID, " started as wheel ", $child->ID, ".\n" );

    push @{ $self->{childs} }, $child;
}

sub _start {
    $_[KERNEL]->sig( INT => 'got_sig_int' );
}

sub _on_child_stdout {
    my ( $stdout_line, $wheel_id ) = @_[ ARG0, ARG1 ];
    my $child = $_[HEAP]{children_by_wid}{$wheel_id};
    print "pid ", $child->PID, " STDOUT: $stdout_line\n";
}

sub _on_child_stderr {
    my ( $stderr_line, $wheel_id ) = @_[ ARG0, ARG1 ];
    my $child = $_[HEAP]{children_by_wid}{$wheel_id};
    print "pid ", $child->PID, " STDERR: $stderr_line\n";
}

sub _on_child_close {
    my $wheel_id = $_[ARG0];
    my $child    = delete $_[HEAP]{children_by_wid}{$wheel_id};

    # May have been reaped by on_child_signal().
    unless ( defined $child ) {
        print "wid $wheel_id closed all pipes.\n";
        return;
    }

    print "pid ", $child->PID, " closed all pipes.\n";
    delete $_[HEAP]{children_by_pid}{ $child->PID };
}

sub _on_child_signal {
    print "pid $_[ARG1] exited with status $_[ARG2].\n";
    my $child = delete $_[HEAP]{children_by_pid}{ $_[ARG1] };

    # May have been reaped by on_child_close().
    return unless defined $child;

    delete $_[HEAP]{children_by_wid}{ $child->ID };
}

sub _on_sig_int {
    foreach my $pid ( keys %{ $_[HEAP]{children_by_pid} } ) {
        my $child = $_[HEAP]{children_by_pid}{$pid};
        warn "Killing child PID: $pid";
        $child->kill();
    }
    $_[KERNEL]->sig_handled();
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
