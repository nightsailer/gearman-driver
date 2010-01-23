use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver;
use POE;

POE::Kernel->run();

my $driver = Gearman::Driver->new( interval => 0 );
my %params = ();

throws_ok { $driver->add_job( \%params ) }
qr/Attribute \(max_processes\) does not pass the type constraint because: Validation failed for 'Int' failed with value undef/,
  'max_processes undef';

$params{max_processes} = 1;
throws_ok { $driver->add_job( \%params ) }
qr/Attribute \(method\) does not pass the type constraint because: Validation failed for 'CodeRef' failed with value undef/,
  'method undef';

$params{method} = sub { };
throws_ok { $driver->add_job( \%params ) }
qr/Attribute \(name\) does not pass the type constraint because: Validation failed for 'Str' failed with value undef/,
  'name undef';

$params{name} = 'some_job';
throws_ok { $driver->add_job( \%params ) }
qr/Attribute \(min_processes\) does not pass the type constraint because: Validation failed for 'Int' failed with value undef/,
  'min_processes undef';

$params{min_processes} = 0;
ok( $driver->add_job( \%params ), 'add_job successful' );
