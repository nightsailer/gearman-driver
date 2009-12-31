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
        my ( $self, $job, $workload ) = @_;
        # called before each job
    }

    sub prefix {
        # default: return ref(shift) . '::';
        return join '_', split /::/, __PACKAGE__;
    }

    sub do_something : Job : MinChilds(2) : MaxChilds(15) {
        my ( $self, $job, $workload ) = @_;
        # $job => Gearman::XS::Job instance
    }

    sub end {
        my ( $self, $job, $workload ) = @_;
        # called after each job
    }

    sub spread_work : Job {
        my ( $self, $job, $workload ) = @_;

        my $gc = Gearman::XS::Client->new;
        $gc->add_servers( $self->server );

        $gc->do_background( 'some_job_1' => $job->workload );
        $gc->do_background( 'some_job_2' => $job->workload );
        $gc->do_background( 'some_job_3' => $job->workload );
        $gc->do_background( 'some_job_4' => $job->workload );
        $gc->do_background( 'some_job_5' => $job->workload );
    }

    1;

=head1 DESCRIPTION

=head1 ATTRIBUTES

=head2 server

L<Gearman::Driver> connects to the L<server|Gearman::Driver/server>
passed to its constructor. This value is also stored in this class.
This can be useful if a job uses L<Gearman::XS::Client> to add
another jobs. See 'spread_work' method in L</SYNOPSIS> above.

=cut

has 'server' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

=head1 METHODATTRIBUTES

=head2 Job

This will register the method with gearmand.

=head2 MinChilds

Minimum number of childs working parallel on this job/method.

=head2 MaxChilds

Maximum number of childs working parallel on this job/method.

=head2 Encode

This will automatically look for a method C<encode> in this object
which has to be defined in the subclass. It will call the C<encode>
method passing the return value from the job method. The return
value of the C<encode> method will be returned to the Gearman
client. This is useful to serialize Perl datastructures to JSON
before sending them back to the client.

    sub do_some_job : Job : Encode : Decode {
        my ( $self, $job, $workload ) = @_;
        return { message => 'OK', status => 1 };

        # calls 'encode' and returns JSON string: {"status":1,"message":"OK"}
    }

    sub custom_encoder : Job : Encode(enc_yaml) : Decode(dec_yaml) {
        my ( $self, $job, $workload ) = @_;
        return { message => 'OK', status => 1 };

        # calls 'enc_yaml' and returns YAML string:
        # ---
        # message: OK
        # status: 1
    }

    sub encode {
        my ( $self, $result ) = @_;
        return JSON::XS::encode_json($result);
    }

    sub decode {
        my ( $self, $workload ) = @_;
        return JSON::XS::decode_json($workload);
    }

    sub enc_yaml {
        my ( $self, $result ) = @_;
        return YAML::XS::Dump($result);
    }

    sub dec_yaml {
        my ( $self, $workload ) = @_;
        return YAML::XS::Load($workload);
    }


=head2 Decode

This will automatically look for a method C<decode> in this object
which has to be defined in the subclass. It will call the C<decode>
method passing the workload value (C<< $job->workload >>). The return
value of the C<decode> method will be passed as 3rd argument to the
job method. This is useful to deserialize JSON workload to Perl
datastructures for example. If this attribute is not set,
C<< $job->workload >> and C<$workload> is the same.

Example, workload is this string: C<{"status":1,"message":"OK"}>

    sub decode {
        my ( $self, $workload ) = @_;
        return JSON::XS::decode_json($workload);
    }

    sub job1 : Job {
        my ( $self, $job, $workload ) = @_;
        # $workload eq $job->workload eq '{"status":1,"message":"OK"}'
    }

    sub job2 : Job : Decode {
        my ( $self, $job, $workload ) = @_;
        # $workload ne $job->workload
        # $job->workload eq '{"status":1,"message":"OK"}'
        # $workload = { status => 1, message => 'OK' }
    }


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
