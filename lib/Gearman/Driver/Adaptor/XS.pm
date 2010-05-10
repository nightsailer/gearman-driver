package Gearman::Driver::Adaptor::XS;

use Moose;
use Gearman::XS::Worker;
use Gearman::XS qw(:constants);

has 'worker' => (
    builder => '_build_worker',
    is      => 'ro',
    isa     => 'Gearman::XS::Worker',
);

sub _build_worker {
    return Gearman::XS::Worker->new;
}

sub add_function {
    my ( $self, $name, $sub ) = @_;
    my $ret = $self->worker->add_function( $name, 0, $sub, '' );
    if ( $ret != GEARMAN_SUCCESS ) {
        die $self->gearman->error;
    }
}

sub work {
    my ($self) = @_;
    
    $self->worker->add_options(GEARMAN_WORKER_NON_BLOCKING);
    
    while (1) {
        my $ret = $self->worker->work;
        if ($ret == GEARMAN_IO_WAIT || $ret == GEARMAN_NO_JOBS ) {
            $self->worker->wait;
        }
        elsif ($ret != GEARMAN_SUCCESS ) {
            die $self->worker->error;
        }
    }
}

sub add_servers {
    my $self = shift;
    return $self->worker->add_servers(@_);
}

1;
