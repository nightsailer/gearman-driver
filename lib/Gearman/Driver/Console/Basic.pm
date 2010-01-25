package Gearman::Driver::Console::Basic;

use Moose::Role;

=head1 NAME

Gearman::Driver::Console::Basic - Provides basic console commands

=head1 DESCRIPTION

This implements the basic management console commands like C<status>,
C<quit>, C<shutdown>, ...

=head1 COMMANDS

=head2 status

Parameters: C<none>

    GDExamples::Sleeper::ZzZzZzzz   3       6       3
    GDExamples::Sleeper::long_running_ZzZzZzzz      1       2       1
    GDExamples::WWW::is_online      0       1       0
    .

Columns are separated by tabs in this order:

=over 4

=item * job_name

=item * min_processes

=item * max_processes

=item * current_processes

=back

=cut

sub status {
    my ($self) = @_;
    my @result = ();
    foreach my $job ( $self->driver->get_jobs ) {
        push @result,
          sprintf( "%s\t%d\t%d\t%d", $job->name, $job->min_processes, $job->max_processes, $job->count_processes );
    }
    return @result;
}

=head2 set_min_processes

Parameters: C<job_name min_processes>

    set_min_processes asdf 5
    ERR invalid_job_name: asdf
    set_min_processes GDExamples::Sleeper::ZzZzZzzz ten
    ERR invalid_value: min_processes must be >= 0
    set_min_processes GDExamples::Sleeper::ZzZzZzzz 10
    ERR invalid_value: min_processes must be smaller than max_processes
    set_min_processes GDExamples::Sleeper::ZzZzZzzz 5
    OK
    .

=cut

*set_min_childs = \&set_min_processes;

sub set_min_processes {
    my ( $self, $job_name, $min_processes ) = @_;

    my $job = $self->get_job($job_name);

    if ( !defined($min_processes) || $min_processes !~ /^\d+$/ || $min_processes < 0 ) {
        die "ERR invalid_value: min_processes must be >= 0\n";
    }

    if ( $min_processes > $job->max_processes ) {
        die "ERR invalid_value: min_processes must be smaller than max_processes\n";
    }

    $job->min_processes($min_processes);

    return "OK";
}

=head2 set_max_processes

Parameters: C<job_name max_processes>

    set_max_processes asdf 5
    ERR invalid_job_name: asdf
    set_max_processes GDExamples::Sleeper::ZzZzZzzz ten
    ERR invalid_value: max_processes must be >= 0
    set_max_processes GDExamples::Sleeper::ZzZzZzzz 0
    ERR invalid_value: max_processes must be greater than min_processes
    set_max_processes GDExamples::Sleeper::ZzZzZzzz 6
    OK
    .

=cut

*set_max_childs = \&set_max_processes;

sub set_max_processes {
    my ( $self, $job_name, $max_processes ) = @_;

    my $job = $self->get_job($job_name);

    if ( !defined($max_processes) || $max_processes !~ /^\d+$/ || $max_processes < 0 ) {
        die "ERR invalid_value: max_processes must be >= 0\n";
    }

    if ( $max_processes < $job->min_processes ) {
        die "ERR invalid_value: max_processes must be greater than min_processes\n";
    }

    $job->max_processes($max_processes);

    return "OK";
}

=head2 quit

Parameters: C<none>

Closes your connection gracefully.

=head2 shutdown

Parameters: C<none>

Shuts L<Gearman::Driver> down.

=cut

sub shutdown {
    my ($self) = @_;
    $self->driver->shutdown;
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

=item * L<Gearman::Driver::Console>

=item * L<Gearman::Driver::Job>

=item * L<Gearman::Driver::Observer>

=item * L<Gearman::Driver::Worker>

=back

=cut

1;