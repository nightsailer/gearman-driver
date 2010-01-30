use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver;
use POE;
use Loader::Empty;

{
    my $e = Loader::Empty->new();
    isa_ok( $e, 'Loader::Empty', 'Role works' );
    lives_ok { $e->namespaces( [qw(Live)] ) } 'namespaces method/attribute';
    is_deeply( [ $e->get_namespaces ], [qw(Live)], 'get_namespaces method/attribute' );
    lives_ok {
        $e->wanted(
            sub {
                return 1 if /Begin|Basic|AddJob/;
                return 0;
            }
        );
    }
    'wanted method/attribute';
    lives_ok { $e->load_namespaces } 'load_namespaces method/attribute';
    is_deeply(
        [ $e->get_modules ],
        [ 'Live::NS1::Basic', 'Live::NS1::BasicChilds', 'Live::NS1::BeginEnd', 'Live::NS2::BeginEnd' ],
        'get_modules method/attribute'
    );

}
exit;
POE::Kernel->run();

{
    my $driver = Gearman::Driver->new(
        interval   => 0,
        namespaces => [qw(Live)],
    );

    $driver->load_namespaces;

    is_deeply(
        [ $driver->get_modules ],
        [
            'Live',                               'Live::NS1::Basic',
            'Live::NS1::BasicChilds',             'Live::NS1::BeginEnd',
            'Live::NS1::Decode',                  'Live::NS1::DefaultAttributes',
            'Live::NS1::DefaultAttributesChilds', 'Live::NS1::Encode',
            'Live::NS1::OverrideAttributes',      'Live::NS1::OverrideAttributesChilds',
            'Live::NS1::Spread',                  'Live::NS2::BeginEnd',
            'Live::NS2::Ping1',                   'Live::NS2::Ping2',
            'Live::NS2::UseBase'
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

    $driver->load_namespaces;

    is_deeply(
        [ $driver->get_modules ],
        [ 'Live::NS2::BeginEnd', 'Live::NS2::Ping1', 'Live::NS2::Ping2', 'Live::NS2::UseBase' ],
        'load namespaces with filter'
    );
}

{
    my $driver = Gearman::Driver->new(
        interval   => 0,
        namespaces => [qw(DoesNotExist)],
    );

    $driver->load_namespaces;

    is_deeply( [ $driver->get_modules ], [], 'empty namespace' );
}
