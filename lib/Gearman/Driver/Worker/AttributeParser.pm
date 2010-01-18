package Gearman::Driver::Worker::AttributeParser;

use Moose::Role;

has 'parsed_attributes' => (
    builder => '_parse_attributes',
    handles => {
        has_attribute => 'defined',
        get_attribute => 'get',
    },
    is     => 'ro',
    isa    => 'HashRef',
    lazy   => 1,
    traits => [qw(Hash)],
);

has 'default_attributes' => (
    default => sub { {} },
    is      => 'rw',
    isa     => 'HashRef',
);

has 'override_attributes' => (
    default => sub { {} },
    is      => 'rw',
    isa     => 'HashRef',
);

sub _parse_attributes {
    my ($self) = @_;

    my $attributes = $self->attributes;

    my @valid_attributes = qw(MinChilds MaxChilds Job Encode Decode);

    my $result = {
        Decode    => 0,
        Encode    => 0,
        Job       => 0,
        MinChilds => 1,
        MaxChilds => 1,
    };

    foreach my $attr ( keys %{ $self->default_attributes } ) {
        unshift @$attributes, sprintf '%s(%s)', $attr, $self->default_attributes->{$attr};
    }

    foreach my $attr ( keys %{ $self->override_attributes } ) {
        push @$attributes, sprintf '%s(%s)', $attr, $self->override_attributes->{$attr};
    }

    foreach my $attr (@$attributes) {
        my ( $type, $value ) = $attr =~ / (\w+) (?: \( (.*?) \) )*/x;

        # Default values
        $value ||= 'encode' if $type eq 'Encode';
        $value ||= 'decode' if $type eq 'Decode';
        $value = 1 unless defined $value;

        unless ( grep $type eq $_, @valid_attributes ) {
            warn "Invalid attribute '$attr' in " . ref($self);
            next;
        }

        $result->{$type} = $value if defined $result->{$type};
    }

    return $result;
}

no Moose::Role;

1;
