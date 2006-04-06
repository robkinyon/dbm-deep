##
# DBM::Deep Test
##
use strict;
use Test::More tests => 14;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
);

##
# large keys
##
my $key1 = "Now is the time for all good men to come to the aid of their country." x 100;
my $key2 = "The quick brown fox jumped over the lazy, sleeping dog." x 1000;
my $key3 = "Lorem dolor ipsum latinum suckum causum Ium cannotum rememberum squatum." x 1000;

$db->put($key1, "value1");
$db->store($key2, "value2");
$db->{$key3} = "value3";

is( $db->{$key1}, 'value1', "Hash retrieval of put()" );
is( $db->{$key2}, 'value2', "Hash retrieval of store()" );
is( $db->{$key3}, 'value3', "Hash retrieval of hashstore" );
is( $db->get($key1), 'value1', "get() retrieval of put()" );
is( $db->get($key2), 'value2', "get() retrieval of store()" );
is( $db->get($key3), 'value3', "get() retrieval of hashstore" );
is( $db->fetch($key1), 'value1', "fetch() retrieval of put()" );
is( $db->fetch($key2), 'value2', "fetch() retrieval of store()" );
is( $db->fetch($key3), 'value3', "fetch() retrieval of hashstore" );

my $test_key = $db->first_key();
ok(
	($test_key eq $key1) || 
	($test_key eq $key2) || 
	($test_key eq $key3)
);

$test_key = $db->next_key($test_key);
ok(
	($test_key eq $key1) || 
	($test_key eq $key2) || 
	($test_key eq $key3)
);

$test_key = $db->next_key($test_key);
ok(
	($test_key eq $key1) || 
	($test_key eq $key2) || 
	($test_key eq $key3)
);

$test_key = $db->next_key($test_key);
ok( !$test_key );
