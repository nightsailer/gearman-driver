package Gearman::Driver::Worker;

use base qw(MooseX::MethodAttributes::Inheritable);
use Moose;

=head1 NAME

Gearman::Driver::Worker - Base class for workers

=head1 SYNOPSIS

    package My::Worker;

    use base qw(Gearman::Driver::Worker);
    use Moose;

    sub prefix {
        # default: return ref(shift) . '::';
        return join '_', split /::/, __PACKAGE__;
    }

    sub do_something : Job : MinProcs(2) : MaxProcs(15) {
        my ( $self, $driver, $job ) = @_;
        # $driver => Gearman::Driver instance
        # $job => Gearman::XS::Job instance
    }

    1;

=head1 DESCRIPTION

=head1 METHODS

=head2 prefix

Having the same method name in two different classes would result
in a clash when registering it on the Gearman server. To avoid this,
all jobs are registered with the full package/class and method name
(e.g. C<My::Worker::some_job>). The default prefix is
C<ref(shift . '::')>, but it can be overridden by overriding the
C<prefix> method in the subclass.

=cut

sub prefix {
    return ref(shift) . '::';
}

sub _parse_attributes {
    my ( $self, $attributes ) = @_;

    my @valid_attributes = qw(MinProcs MaxProcs Job);

    my $result = {
        MinProcs => 1,
        MaxProcs => 1,
    };

    foreach my $attr (@$attributes) {
        my ( $type, $value ) = $attr =~ / (\w+) (?: \( (\d+) \) )*/x;
        unless ( grep $type eq $_, @valid_attributes ) {
            warn "Invalid attribute '$attr' in " . ref($self);
            next;
        }
        $result->{$type} = $value if defined $result->{$type};
    }

    return $result;
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
