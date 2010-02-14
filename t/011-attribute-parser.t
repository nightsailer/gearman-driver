use strict;
use warnings;
use Test::More tests => 12;
use Test::Differences;
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Driver::Worker::AttributeParser;
use Moose::Util qw(apply_all_roles);
use Gearman::Driver::Test::Live::NS1::Basic;
use Gearman::Driver::Test::Live::NS1::Decode;
use Gearman::Driver::Test::Live::NS1::Encode;
use Gearman::Driver::Test::Live::NS1::DefaultAttributes;
use Gearman::Driver::Test::Live::NS1::OverrideAttributes;

{
    my %expected = (
        'get_pid' => {
            'Job'          => 1,
            'MinProcesses' => '0'
        },
        'ping'    => { 'Job' => 1 },
        'quit'    => { 'Job' => 1 },
        'sleeper' => {
            'Job'          => 1,
            'MaxProcesses' => '6',
            'MinProcesses' => '2'
        },
        'sleepy_pid' => {
            'Job'          => 1,
            'MinProcesses' => '0'
        },
        'ten_processes' => {
            'Job'          => 1,
            'MaxProcesses' => '10',
            'MinProcesses' => '10'
        }
    );
    my $worker = Gearman::Driver::Test::Live::NS1::Basic->new();
    foreach my $method ( $worker->meta->get_nearest_methods_with_attributes ) {
        apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );
        eq_or_diff( $method->parsed_attributes, $expected{ $method->name } );
    }
}

{
    my %expected = (
        'job1' => {
            'Decode' => 'decode',
            'Job'    => 1
        },
        'job2' => {
            'Decode' => 'custom_decode',
            'Job'    => 1
        }
    );
    my $worker = Gearman::Driver::Test::Live::NS1::Decode->new();
    foreach my $method ( $worker->meta->get_nearest_methods_with_attributes ) {
        apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );
        eq_or_diff( $method->parsed_attributes, $expected{ $method->name } );
    }
}

{
    my %expected = (
        'job1' => {
            'Encode' => 'encode',
            'Job'    => 1
        },
        'job2' => {
            'Encode' => 'custom_encode',
            'Job'    => 1
        }
    );
    my $worker = Gearman::Driver::Test::Live::NS1::Encode->new();
    foreach my $method ( $worker->meta->get_nearest_methods_with_attributes ) {
        apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );
        eq_or_diff( $method->parsed_attributes, $expected{ $method->name } );
    }
}

{
    my %expected = (
        'job' => {
            'Decode'       => 'decode',
            'Encode'       => 'encode',
            'Job'          => 1,
            'MinProcesses' => '3'
        }
    );
    my $worker = Gearman::Driver::Test::Live::NS1::DefaultAttributes->new();
    foreach my $method ( $worker->meta->get_nearest_methods_with_attributes ) {
        apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );
        $method->default_attributes( $worker->default_attributes );
        $method->override_attributes( $worker->override_attributes );
        eq_or_diff( $method->parsed_attributes, $expected{ $method->name } );
    }
}

{
    my %expected = (
        'job' => {
            'Decode'       => 'decode',
            'Encode'       => 'encode',
            'Job'          => 1,
            'MinProcesses' => '1'
        }
    );
    my $worker = Gearman::Driver::Test::Live::NS1::OverrideAttributes->new();
    foreach my $method ( $worker->meta->get_nearest_methods_with_attributes ) {
        apply_all_roles( $method => 'Gearman::Driver::Worker::AttributeParser' );
        $method->default_attributes( $worker->default_attributes );
        $method->override_attributes( $worker->override_attributes );
        eq_or_diff( $method->parsed_attributes, $expected{ $method->name } );
    }
}
