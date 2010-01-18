use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver;
use POE;

throws_ok { Gearman::Driver->new() }
qr/Attribute \(namespaces\) is required/,
  'zero caught okay';

throws_ok { Gearman::Driver->new( namespaces => [qw(Foo Bar Bla::Fasel)] ) }
qr/Could not find any modules in those namespaces: Bar, Bla::Fasel, Foo/,
  'No modules found';

throws_ok { Gearman::Driver->new( namespaces => [qw(Validate::Invalid)] ) }
qr/None of the modules have a method with 'Job' attribute set: Validate::Invalid::NS1::Something, Validate::Invalid::NS2::Something1, Validate::Invalid::NS2::Something2, Validate::Invalid::NS2::SubNS::Something, Validate::Invalid::Something/,
  'None of the modules found have correct inheritance';

throws_ok { Gearman::Driver->new( namespaces => [qw(Validate::NoJobs)] ) }
qr/None of the modules have a method with 'Job' attribute set: Validate::NoJobs::NS1::Something, Validate::NoJobs::Something/,
  'None of the modules found have correct job methods';
