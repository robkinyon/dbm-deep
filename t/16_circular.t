##
# DBM::Deep Test
##
use strict;
use Test::More tests => 19;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
my $db = DBM::Deep->new( $filename );

##
# put/get simple keys
##
$db->{key1} = "value1";
$db->{key2} = "value2";

my @keys_1 = sort keys %$db;

$db->{key3} = $db->{key1};

my @keys_2 = sort keys %$db;
is( @keys_2 + 0, @keys_1 + 1, "Correct number of keys" );
is_deeply(
    [ @keys_1, 'key3' ],
    [ @keys_2 ],
    "Keys still match after circular reference is added",
);

$db->{key4} = {};
$db->{key5} = $db->{key4};

my @keys_3 = sort keys %$db;

TODO: {
    local $TODO = "Need to fix how internal references are stored";
    is( @keys_3 + 0, @keys_2 + 2, "Correct number of keys" );
    is_deeply(
        [ @keys_2, 'key4', 'key5' ],
        [ @keys_3 ],
        "Keys still match after circular reference is added (@keys_3)",
    );

    ##
    # Insert circular reference
    ##
    $db->{circle} = $db;

    my @keys_4 = sort keys %$db;
    print "@keys_4\n";

    is( @keys_4 + 0, @keys_3 + 1, "Correct number of keys" );
    is_deeply(
        [ '[base]', @keys_3 ],
        [ @keys_4 ],
        "Keys still match after circular reference is added",
    );
}

##
# Make sure keys exist in both places
##
is( $db->{key1}, 'value1', "The value is there directly" );
is( $db->{circle}{key1}, 'value1', "The value is there in one loop of the circle" );
is( $db->{circle}{circle}{key1}, 'value1', "The value is there in two loops of the circle" );
is( $db->{circle}{circle}{circle}{key1}, 'value1', "The value is there in three loops of the circle" );

##
# Make sure changes are reflected in both places
##
$db->{key1} = "another value";

is( $db->{key1}, 'another value', "The value is there directly" );
is( $db->{circle}{key1}, 'another value', "The value is there in one loop of the circle" );
is( $db->{circle}{circle}{key1}, 'another value', "The value is there in two loops of the circle" );
is( $db->{circle}{circle}{circle}{key1}, 'another value', "The value is there in three loops of the circle" );

$db->{circle}{circle}{circle}{circle}{key1} = "circles";

is( $db->{key1}, 'circles', "The value is there directly" );
is( $db->{circle}{key1}, 'circles', "The value is there in one loop of the circle" );
is( $db->{circle}{circle}{key1}, 'circles', "The value is there in two loops of the circle" );
is( $db->{circle}{circle}{circle}{key1}, 'circles', "The value is there in three loops of the circle" );
