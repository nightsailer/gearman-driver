use strict;
use warnings;
use Test::More tests => 6;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver;
use POE;

eval { Gearman::Driver->new(); };

like( $@, qr/Attribute \(namespaces\) is required/, 'Mandatory parameters missing' );

eval { Gearman::Driver->new( namespaces => [qw(Foo Bar Bla::Fasel)] ); };

like( $@, qr/Could not find any modules in those namespaces: Bar, Bla::Fasel, Foo/, 'No modules found' );

eval { Gearman::Driver->new( namespaces => [qw(Validate::Invalid)] ); };

like(
    $@,
qr/None of the modules have a method with 'Job' attribute set: Validate::Invalid::Worker, Validate::Invalid::NS2::Worker1, Validate::Invalid::NS2::Worker2, Validate::Invalid::NS2::SubNS::Worker, Validate::Invalid::NS1::Worker/,
    'None of the modules found have correct inheritance'
);

eval { Gearman::Driver->new( namespaces => [qw(Validate::NoJobs)] ); };

like(
    $@,
qr/None of the modules have a method with 'Job' attribute set: Validate::NoJobs::Worker, Validate::NoJobs::NS1::Worker/,
    'None of the modules found have correct job methods'
);

# Make POE happy, do not run the event loop
POE::Kernel->run;

my $driver = Gearman::Driver->new( namespaces => [qw(Validate::Valid)] );

isa_ok( $driver, 'Gearman::Driver', 'Object instantiation with valid parameters' );

my @expected_modules = qw(
  Validate::Valid::NS1::Worker
  Validate::Valid::NS2::SubNS::Worker
  Validate::Valid::NS2::Worker
);
is_deeply( [ $driver->all_modules ], \@expected_modules, 'All expected modules were loaded' );