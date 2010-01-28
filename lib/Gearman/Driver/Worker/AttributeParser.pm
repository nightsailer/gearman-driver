package Gearman::Driver::Worker::AttributeParser;

use Moose::Role;

=head1 NAME

Gearman::Driver::Worker::AttributeParser - Parses worker attributes

=head1 DESCRIPTION

This module is responsible for parsing the
L<method attributes|Gearman::Driver::Worker/METHODATTRIBUTES>
of a worker. It currently has no public interface.

=cut

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

    my @valid_attributes = qw(
      Encode Decode Job MaxChilds
      MaxProcesses MinChilds MinProcesses
    );

    my $result = {
        Decode       => 0,
        Encode       => 0,
        Job          => 0,
        MinProcesses => 1,
        MaxProcesses => 1,
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

        $type =~ s/^(Min|Max)Childs$/$1Processes/;

        $result->{$type} = $value if defined $result->{$type};
    }

    return $result;
}

no Moose::Role;

=head1 AUTHOR

See L<Gearman::Driver>.

=head1 COPYRIGHT AND LICENSE

See L<Gearman::Driver>.

=head1 SEE ALSO

=over 4

=item * L<Gearman::Driver>

=item * L<Gearman::Driver::Console>

=item * L<Gearman::Driver::Console::Basic>

=item * L<Gearman::Driver::Job>

=item * L<Gearman::Driver::Loader>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=item * L<Gearman::Driver::Worker::Base>

=back

=cut

1;
