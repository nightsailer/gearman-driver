use inc::Module::Install 0.91;

if ( -e 'MANIFEST.SKIP' ) {
    system('pod2text lib/Gearman/Driver.pm > README');
}

name 'Gearman-Driver';
all_from 'lib/Gearman/Driver.pm';

requires(
    'Gearman::XS'                => '0',
    'Module::Find'               => '0',
    'Moose'                      => '0',
    'MooseX::Getopt'             => '0',
    'MooseX::Log::Log4perl'      => '0',
    'MooseX::MethodAttributes'   => '0',
    'MooseX::Types::Path::Class' => '0',
    'Net::Telnet::Gearman'       => '0',
    'POE'                        => '0',
);

test_requires(
    'Test::More' => 0,
    'File::Temp' => 0,
);

auto_install;
WriteAll;
