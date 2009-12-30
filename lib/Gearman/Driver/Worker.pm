package Gearman::Driver::Worker;

use base qw(MooseX::MethodAttributes::Inheritable);
use Moose;

=head1 NAME

Gearman::Driver::Worker - Base class for workers

=head1 SYNOPSIS

    package My::Worker;

    use base qw(Gearman::Driver::Worker);
    use Moose;

    sub begin {
        # called before each job
    }

    sub prefix {
        # default: return ref(shift) . '::';
        return join '_', split /::/, __PACKAGE__;
    }

    sub do_something : Job : MinChilds(2) : MaxChilds(15) {
        my ( $self, $job ) = @_;
        # $job => Gearman::XS::Job instance
    }

    sub end {
        # called after each job
    }

    1;

=head1 DESCRIPTION

=head1 METHODATTRIBUTES

=head2 Job

This will register the method with gearmand.

=head2 MinChilds

Minimum number of childs working parallel on this job/method.

=head2 MaxChilds

Maximum number of childs working parallel on this job/method.

=head1 METHODS

=head2 prefix

Having the same method name in two different classes would result
in a clash when registering it with gearmand. To avoid this,
all jobs are registered with the full package and method name
(e.g. C<My::Worker::some_job>). The default prefix is
C<ref(shift . '::')>, but this can be changed by overriding the
C<prefix> method in the subclass, see L</SYNOPSIS> above.

=cut

sub prefix {
    return ref(shift) . '::';
}

=head2 begin

This method is called before a job method is called. In this base
class this methods just does nothing, but can be overridden in a
subclass.

The parameters are the same as in the job method:

=over 4

=item * C<$self>

=item * C<$job>

=back

=cut

sub begin { }

=head2 end

This method is called after a job method has been called. In this
base class this methods just does nothing, but can be overridden
in a subclass.

The parameters are the same as in the job method:

=over 4

=item * C<$self>

=item * C<$job>

=back

=cut

sub end { }

sub _parse_attributes {
    my ( $self, $attributes ) = @_;

    my @valid_attributes = qw(MinChilds MaxChilds Job);

    my $result = {
        MinChilds => 1,
        MaxChilds => 1,
    };

    foreach my $attr (@$attributes) {
        my ( $type, $value ) = $attr =~ / (\w+) (?: \( (\d+) \) )*/x;
        $value ||= 1;
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

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Job>

=back

=cut

1;
