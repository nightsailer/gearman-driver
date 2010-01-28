#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use Gearman::XS::Client;

# /usr/local/sbin/gearmand -d
# ./examples/driver.pl --loglevel DEBUG &
# tail -f gearman_driver.log &
# ./examples/client.pl

my $c      = Gearman::XS::Client->new;
my @result = ();

$c->add_servers('localhost:4730');

for (qw(http://www.google.de http://search.cpan.org http://www.perl.org http://www.github.com)) {
    @result = $c->do( 'GDExamples::WWW::is_online' => $_ );
    printf "%s => %s\n", $_, $result[1] ? 'is online' : 'is offline';
}

$c->do( 'GDExamples::Sleeper::error' => 'something' );

$c->do_background( 'GDExamples::Sleeper::ZzZzZzzz' => 'something' ) for 1 .. 20;
@result = $c->do( 'GDExamples::Sleeper::ZzZzZzzz' => 'something else' );

$c->do_background( 'GDExamples::Sleeper::long_running_ZzZzZzzz' => 'something' ) for 1 .. 6;
@result = $c->do( 'GDExamples::Sleeper::long_running_ZzZzZzzz' => 'something else' );

# $c->do_background( 'some-unknown-job' => 'something' ) for 1 .. 3;

