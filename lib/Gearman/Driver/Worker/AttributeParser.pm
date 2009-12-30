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
    traits => [qw(Hash)],
);

sub _parse_attributes {
    my ($self) = @_;

    my $attributes = $self->attributes;

    my @valid_attributes = qw(MinChilds MaxChilds Job Encode);

    my $result = {
        Encode    => 0,
        Job       => 0,
        MinChilds => 1,
        MaxChilds => 1,
    };

    foreach my $attr (@$attributes) {
        my ( $type, $value ) = $attr =~ / (\w+) (?: \( (.*?) \) )*/x;

        # Default values
        $value ||= 'encode' if $type eq 'Encode';
        $value ||= 1;

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
