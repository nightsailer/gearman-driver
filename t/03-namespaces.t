use strict;
use warnings;
use Test::More tests => 2;
use Test::Exception;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver;
use POE;

POE::Kernel->run();

{
    my $driver = Gearman::Driver->new(
        interval   => 0,
        namespaces => [qw(Live)],
    );

    $driver->_load_namespaces;

    is_deeply(
        [ $driver->get_modules ],
        [
            'Live',                          'Live::NS1::Basic',
            'Live::NS1::BeginEnd',           'Live::NS1::Decode',
            'Live::NS1::DefaultAttributes',  'Live::NS1::Encode',
            'Live::NS1::OverrideAttributes', 'Live::NS1::Spread',
            'Live::NS2::BeginEnd',           'Live::NS2::Ping1',
            'Live::NS2::Ping2',              'Live::NS2::UseBase'
        ],
        'load namespaces without filter'
    );
}

{
    my $driver = Gearman::Driver->new(
        interval   => 0,
        namespaces => [qw(Live)],
        wanted     => sub {
            return 1 if /NS2/;
            return 0;
        },
    );

    $driver->_load_namespaces;

    is_deeply(
        [ $driver->get_modules ],
        [ 'Live::NS2::BeginEnd', 'Live::NS2::Ping1', 'Live::NS2::Ping2', 'Live::NS2::UseBase' ],
        'load namespaces with filter'
    );
}
