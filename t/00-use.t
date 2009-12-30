use Test::More tests => 4;

BEGIN {
    use_ok('Gearman::Driver');
    use_ok('Gearman::Driver::Observer');
    use_ok('Gearman::Driver::Job');
    use_ok('Gearman::Driver::Worker');
}

diag("Testing Gearman::Driver $Gearman::Driver::VERSION");
