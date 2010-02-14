package Job;

use Moose;

has 'workload' => (
    default => '',
    is      => 'rw',
    isa     => 'Str',
);

package main;

use strict;
use warnings;
use Test::More tests => 1;
use FindBin;
use lib "$FindBin::Bin/lib";
use Class::MOP::Class;
use Gearman::Driver::Job::Method;
use Gearman::Driver::Worker::Base;

my $worker = Gearman::Driver::Worker::Base->new();

{
    my $m = Gearman::Driver::Job::Method->new(
        name => 'test',
        body => sub {
            return 123;
        },
        worker => $worker,
    );
    my $result = $m->wrapper->( Job->new );
    is( $result, 123, 'Basic result without any magic' );
}
