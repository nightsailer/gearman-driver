package Gearman::Driver::Worker::Base;

use Moose;

has 'server' => (
    is  => 'ro',
    isa => 'Str',
);

sub prefix {
    return ref(shift) . '::';
}

sub begin { }

sub end { }

sub process_name {
    return 0;
}

sub override_attributes {
    return {};
}

sub default_attributes {
    return {};
}

sub decode {
    my ( $self, $result ) = @_;
    return $result;
}

sub encode {
    my ( $self, $result ) = @_;
    return $result;
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;
