##
# DBM::Deep Test
##
use strict;
use Test::More tests => 32;
use Test::Exception;
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
flock $fh, LOCK_UN;
my $db = DBM::Deep->new( $filename );

##
# put/get key
##
$db->{key1} = "value1";
is( $db->get("key1"), "value1", "get() works with hash assignment" );
is( $db->fetch("key1"), "value1", "... fetch() works with hash assignment" );
is( $db->{key1}, "value1", "... and hash-access also works" );

$db->put("key2", undef);
is( $db->get("key2"), undef, "get() works with put()" );
is( $db->fetch("key2"), undef, "... fetch() works with put()" );
is( $db->{key2}, undef, "... and hash-access also works" );

$db->store( "key3", "value3" );
is( $db->get("key3"), "value3", "get() works with store()" );
is( $db->fetch("key3"), "value3", "... fetch() works with put()" );
is( $db->{key3}, 'value3', "... and hash-access also works" );

ok( $db->exists("key1"), "exists() function works" );
ok( exists $db->{key2}, "exists() works against tied hash" );

##
# count keys
##
is( scalar keys %$db, 3, "keys() works against tied hash" );

##
# step through keys
##
my $temphash = {};
while ( my ($key, $value) = each %$db ) {
	$temphash->{$key} = $value;
}

is( $temphash->{key1}, 'value1', "First key copied successfully using tied interface" );
is( $temphash->{key2}, undef, "Second key copied successfully" );
is( $temphash->{key3}, 'value3', "Third key copied successfully" );

$temphash = {};
my $key = $db->first_key();
while ($key) {
	$temphash->{$key} = $db->get($key);
	$key = $db->next_key($key);
}

is( $temphash->{key1}, 'value1', "First key copied successfully using OO interface" );
is( $temphash->{key2}, undef, "Second key copied successfully" );
is( $temphash->{key3}, 'value3', "Third key copied successfully" );

##
# delete keys
##
is( delete $db->{key2}, undef, "delete through tied inteface works" );
is( $db->delete("key1"), 'value1', "delete through OO inteface works" );
is( $db->{key3}, 'value3', "The other key is still there" );

is( scalar keys %$db, 1, "After deleting two keys, 1 remains" );

##
# delete all keys
##
ok( $db->clear(), "clear() returns true" );

is( scalar keys %$db, 0, "After clear(), everything is removed" );

##
# replace key
##
$db->put("key1", "value1");
is( $db->get("key1"), "value1", "Assignment still works" );

$db->put("key1", "value2");
is( $db->get("key1"), "value2", "... and replacement works" );

$db->put("key1", "value222222222222222222222222");

is( $db->get("key1"), "value222222222222222222222222", "We set a value before closing the file" );

##
# Make sure DB still works after closing / opening
##
undef $db;
$db = DBM::Deep->new( $filename );
is( $db->get("key1"), "value222222222222222222222222", "The value we set is still there after closure" );

##
# Make sure keys are still fetchable after replacing values
# with smaller ones (bug found by John Cardenas, DBM::Deep 0.93)
##
$db->clear();
$db->put("key1", "long value here");
$db->put("key2", "longer value here");

$db->put("key1", "short value");
$db->put("key2", "shorter v");

my $first_key = $db->first_key();
my $next_key = $db->next_key($first_key);

ok(
	(($first_key eq "key1") || ($first_key eq "key2")) && 
	(($next_key eq "key1") || ($next_key eq "key2")) && 
	($first_key ne $next_key)
    ,"keys() still works if you replace long values with shorter ones"
);

# Test autovivification

$db->{unknown}{bar} = 1;
ok( $db->{unknown} );
cmp_ok( $db->{unknown}{bar}, '==', 1 );
