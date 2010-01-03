use strict;
use warnings;
use Test::More tests => 4;
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
qr/None of the modules have a method with 'Job' attribute set: Validate::Invalid::NS1::Something, Validate::Invalid::NS2::Something1, Validate::Invalid::NS2::Something2, Validate::Invalid::NS2::SubNS::Something, Validate::Invalid::Something/,
    'None of the modules found have correct inheritance'
);

eval { Gearman::Driver->new( namespaces => [qw(Validate::NoJobs)] ); };

like(
    $@,
qr/None of the modules have a method with 'Job' attribute set: Validate::NoJobs::NS1::Something, Validate::NoJobs::Something/,
    'None of the modules found have correct job methods'
);
