package Gearman::Driver::Job;

use Moose;
use Gearman::Driver::Adaptor;
use POE qw(Wheel::Run);
use IO::Socket;

=head1 NAME

Gearman::Driver::Job - Handles the POE magic

=head1 DESCRIPTION

This class is responsible for starting/stopping processes as well as
handling all pipes (STDOUT/STDERR/STDIN) of the processes. All events
are written to a logfile. Possible events are:

=over 4

=item * Starting processes

=item * STDOUT of processes

=item * STDERR of processes

=item * Stopping processes

=back

The current interface may only be interesting for people subclassing
L<Gearman::Driver> or for people writing commands/extensions for
L<Gearman::Driver::Console>.

=head1 ATTRIBUTES

=head2 driver

Reference to the L<Gearman::Driver> instance.

=cut

has 'driver' => (
    handles  => { log => 'log' },
    is       => 'rw',
    isa      => 'Gearman::Driver',
    required => 1,
    weak_ref => 1,
);

=head2 name

The job's name.

=cut

has 'name' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

=head2 method

Code reference which is run called by L<Gearman::XS::Worker>.
Actually it's not called directly by it, but in a wrapped coderef.

=cut

has 'method' => (
    is       => 'rw',
    isa      => 'CodeRef',
    required => 1,
);

=head2 worker

Reference to the worker object.

=cut

has 'worker' => (
    is       => 'rw',
    isa      => 'Any',
    required => 1,
);

=head2 max_processes

Maximum number of concurrent processes this job may have.

=cut

has 'max_processes' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

=head2 min_processes

Minimum number of concurrent processes this job may have.

=cut

has 'min_processes' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

=head2 encode

This may be set to a method name which is implemented in the worker
class or any subclass. If the method is not available, it will fail.
The returned value of the job method is passed to this method and
the return value of this method is sent back to the Gearman server.

See also: L<Gearman::Driver::Worker/Encode>.

=cut

has 'encode' => (
    default  => '',
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

=head2 decode

This may be set to a method name which is implemented in the worker
class or any subclass. If the method is not available, it will fail.
The workload from L<Gearman::XS::Job> is passed to this method and
the return value is passed as argument C<$workload> to the job
method.

See also: L<Gearman::Driver::Worker/Decode>.

=cut

has 'decode' => (
    default  => '',
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

=head2 processes

This attribute stores a key/value pair containing:
C<$pid> => L<$job|Gearman::Driver::Job>

It provides following methods:

=over 4

=item * C<count_processes()>

=item * C<delete_process($pid)>

=item * C<get_process($pid)>

=item * C<get_processes()>

=item * C<get_pids()>

=item * C<set_process($pid => $job)>

=back

=cut

has 'processes' => (
    default => sub { {} },
    handles => {
        count_processes => 'count',
        delete_process  => 'delete',
        get_process     => 'get',
        get_processes   => 'values',
        get_pids        => 'keys',
        set_process     => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

=head2 gearman

Instance of L<Gearman::Driver::Adaptor>.

=cut

has 'gearman' => (
    is  => 'ro',
    isa => 'Gearman::Driver::Adaptor',
);

=head2 session

Instance of L<POE::Session>.

=cut

has 'session' => (
    is  => 'ro',
    isa => 'POE::Session',
);

has 'client' => (
    builder => '_build_client',
    is      => 'ro',
    isa     => 'IO::Socket::UNIX',
    lazy    => 1,
);

=head2 lastrun

Each time this job is called it stores C<time()> in this attribute.

This depends on L<Gearman::Driver/extended_status>.

=cut

has 'lastrun' => (
    is      => 'rw',
    isa     => 'Int',
    trigger => sub {
        my ( $self, $value, $old_value ) = @_;
        $old_value ||= 0;
        return if $value == $old_value;
        my $sock = $self->client;
        print $sock $self->name . " lastrun $value\n";
    }
);

=head2 lasterror

Each time this job failed it stores C<time()> in this attribute.

This depends on L<Gearman::Driver/extended_status>.

=cut

has 'lasterror' => (
    is      => 'rw',
    isa     => 'Int',
    trigger => sub {
        my ( $self, $value, $old_value ) = @_;
        $old_value ||= 0;
        return if $value == $old_value;
        my $sock = $self->client;
        print $sock $self->name . " lasterror $value\n";
    }
);

=head2 lasterror_msg

Each time this job failed it stores the error message in this
attribute.

This depends on L<Gearman::Driver/extended_status>.

=cut

has 'lasterror_msg' => (
    is      => 'rw',
    isa     => 'Str',
    trigger => sub {
        my ( $self, $value, $old_value ) = @_;
        $old_value ||= '';
        return if $value eq $old_value;
        my $sock = $self->client;
        print $sock $self->name . " lasterror_msg $value\n";
    }
);

=head1 METHODS

=head2 add_process

Starts/forks/adds another process of this job.

=cut

sub add_process {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'add_process' );
}

=head2 remove_process

Removes/kills one process of this job.

=cut

sub remove_process {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'remove_process' );
}

sub BUILD {
    my ($self) = @_;

    $self->{gearman} = Gearman::Driver::Adaptor->new( server => $self->driver->server );

    my $extended_status = $self->driver->extended_status;

    my $decoder = sub { shift };
    my $encoder = sub { shift };

    if ( my $decoder_method = $self->decode ) {
        $decoder = sub { return $self->worker->$decoder_method(@_) };
    }
    if ( my $encoder_method = $self->encode ) {
        $encoder = sub { return $self->worker->$encoder_method(@_) };
    }

    my $wrapper = sub {
        my ($job) = @_;

        my @args = ($job);

        push @args, $decoder->( $job->workload );

        $self->worker->begin(@args);

        my $error;
        my $result;
        eval {
            $result = $self->method->( $self->worker, @args );
        };
        if ($@) {
            $error = $@;
            $self->lasterror(time) if $extended_status;
            $self->lasterror_msg($error) if $extended_status;
        }

        $self->lastrun(time) if $extended_status;

        $self->worker->end(@args);

        die $error if $error;

        return $encoder->($result);
    };

    $self->gearman->add_function( $self->name => $wrapper );

    $self->{session} = POE::Session->create(
        object_states => [
            $self => {
                _start             => '_start',
                got_process_stdout => '_on_process_stdout',
                got_process_stderr => '_on_process_stderr',
                got_process_close  => '_on_process_close',
                got_process_signal => '_on_process_signal',
                add_process        => '_add_process',
                remove_process     => '_remove_process',
            }
        ]
    );
}

sub _build_client {
    my ($self) = @_;
    IO::Socket::UNIX->new(
        Peer    => $self->driver->cc_socket,
        Type    => SOCK_STREAM,
        Timeout => 2,
    ) or die $@;
}

sub _start {
    $_[KERNEL]->alias_set( $_[OBJECT]->name );
}

sub _add_process {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
    my $process = POE::Wheel::Run->new(
        Program => sub {
            POE::Kernel->stop();

            if ( my $process_name = $self->worker->process_name( $0, $self->name ) ) {
                $0 = $process_name;
            }

            $self->gearman->work;
        },
        StdoutEvent => "got_process_stdout",
        StderrEvent => "got_process_stderr",
        CloseEvent  => "got_process_close",
    );
    $kernel->sig_child( $process->PID, "got_process_signal" );

    # Wheel events include the wheel's ID.
    $heap->{wheels}{ $process->ID } = $process;

    $self->log->info( sprintf '(%d) [%s] Process started', $process->PID, $self->name );

    $self->set_process( $process->PID => $process );
}

sub _remove_process {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
    my ($pid) = ( $self->get_pids )[0];
    my $process = $self->delete_process($pid);
    $process->kill();
    $self->log->info( sprintf '(%d) [%s] Process killed', $process->PID, $self->name );
}

sub _on_process_stdout {
    my ( $self, $heap, $stdout, $wid ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];
    my $process = $heap->{wheels}{$wid};
    $self->log->info( sprintf '(%d) [%s] STDOUT: %s', $process->PID, $self->name, $stdout );
}

sub _on_process_stderr {
    my ( $self, $heap, $stderr, $wid ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];
    my $process = $heap->{wheels}{$wid};
    $self->log->info( sprintf '(%d) [%s] STDERR: %s', $process->PID, $self->name, $stderr );
}

sub _on_process_close {
    my ( $self, $heap, $wid ) = @_[ OBJECT, HEAP, ARG0 ];

    my $process = delete $heap->{wheels}{$wid};

    # May have been reaped by got_process_signal
    return unless defined $process;

    $self->delete_process( $process->PID );
}

sub _on_process_signal {
    my ( $self, $heap, $pid, $status ) = @_[ OBJECT, HEAP, ARG1 .. ARG2 ];

    my $process = $self->delete_process($pid);

    $self->log->info( sprintf '(%d) [%s] Exited with status %s', $pid, $self->name, $status );

    # May have been reaped by got_process_close
    return unless defined $process;

    delete $heap->{wheels}{ $process->ID };
}

no Moose;

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Gearman::Driver>.

=head1 COPYRIGHT AND LICENSE

See L<Gearman::Driver>.

=head1 SEE ALSO

=over 4

=item * L<Gearman::Driver>

=item * L<Gearman::Driver::Adaptor>

=item * L<Gearman::Driver::Console>

=item * L<Gearman::Driver::Console::Basic>

=item * L<Gearman::Driver::Console::Client>

=item * L<Gearman::Driver::Loader>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;
