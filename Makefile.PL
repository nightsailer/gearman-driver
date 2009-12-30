use inc::Module::Install 0.91;

if ( -e 'MANIFEST.SKIP' ) {
    system('pod2text lib/Gearman/Driver.pm > README');
}

name 'Gearman-Driver';
all_from 'lib/Gearman/Driver.pm';

requires(
    'Gearman::XS'                => '0.7',
    'Module::Find'               => '0.08',
    'Moose'                      => '0.93',
    'MooseX::Getopt'             => '0.26',
    'MooseX::Log::Log4perl'      => '0.40',
    'MooseX::MethodAttributes'   => '0.18',
    'MooseX::Types::Path::Class' => '0.05',
    'Net::Telnet::Gearman'       => '0.01000',
    'POE'                        => '1.280',
);

test_requires(
    'Test::More'  => 0,
    'File::Temp'  => 0,
    'File::Slurp' => 0,
);

auto_install;
WriteAll;
