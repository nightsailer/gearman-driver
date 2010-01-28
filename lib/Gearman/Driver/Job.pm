package Gearman::Driver::Job;

use Moose;
use Gearman::XS::Worker;
use Gearman::XS qw(:constants);
use POE qw(Wheel::Run);
use Try::Tiny;
use Cache::FastMmap;

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
    isa      => 'CodeRef',
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

has 'max_processes' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

has 'min_processes' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Int',
    required => 1,
);

has 'encode' => (
    default  => '',
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'decode' => (
    default  => '',
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

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

has 'gearman' => (
    is  => 'ro',
    isa => 'Gearman::XS::Worker',
);

has 'session' => (
    is  => 'ro',
    isa => 'POE::Session',
);

has 'lastrun' => (
    is      => 'rw',
    isa     => 'Int',
    trigger => sub {
        my ( $self, $value ) = @_;
        my $key = sprintf '%s_lastrun', $self->name;
        $self->cache->set( $key => $value );
    }
);

has 'lasterror' => (
    is      => 'rw',
    isa     => 'Int',
    trigger => sub {
        my ( $self, $value ) = @_;
        my $key = sprintf '%s_lasterror', $self->name;
        $self->cache->set( $key => $value );
    }
);

has 'lasterror_msg' => (
    is      => 'rw',
    isa     => 'Str',
    trigger => sub {
        my ( $self, $value ) = @_;
        my $key = sprintf '%s_lasterror_msg', $self->name;
        $self->cache->set( $key => $value );
    }
);

has 'cache' => (
    default => sub {
        Cache::FastMmap->new(
            share_file  => '/tmp/gearman_driver.cache',
            expire_time => 604800,
        );
    },
    is  => 'ro',
    isa => 'Cache::FastMmap',
);

sub get_lastrun {
    my ($self) = @_;
    return $self->cache->get( $self->name . "_lastrun" ) || 0;
}

sub get_lasterror {
    my ( $self, $job ) = @_;
    return $self->cache->get( $self->name . "_lasterror" ) || 0;
}

sub get_lasterror_msg {
    my ( $self, $job ) = @_;
    return $self->cache->get( $self->name . "_lasterror_msg" ) || '';
}

sub add_process {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'add_process' );
}

sub remove_process {
    my ($self) = @_;
    POE::Kernel->post( $self->session => 'remove_process' );
}

sub BUILD {
    my ($self) = @_;

    $self->{gearman} = Gearman::XS::Worker->new;
    $self->gearman->add_servers( $self->server );

    my $wrapper = sub {
        my ($job) = @_;

        my @args = ($job);

        if ( my $decoder = $self->decode ) {
            push @args, $self->worker->$decoder( $job->workload );
        }
        else {
            push @args, $job->workload;
        }

        $self->worker->begin(@args);

        my $error;
        my $result;
        try {
            $result = $self->method->( $self->worker, @args );
        }
        catch {
            $error = $_;
            $self->lasterror(time);
            $self->lasterror_msg($error);
        };

        $self->lastrun(time);

        $self->worker->end(@args);

        die $error if $error;

        if ( my $encoder = $self->encode ) {
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

sub _start {
    $_[KERNEL]->alias_set( $_[OBJECT]->name );
}

sub _add_process {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
    my $process = POE::Wheel::Run->new(
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

=item * L<Gearman::Driver::Console>

=item * L<Gearman::Driver::Console::Basic>

=item * L<Gearman::Driver::Loader>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;
