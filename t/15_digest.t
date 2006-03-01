##
# DBM::Deep Test
##
use strict;
use Test::More tests => 14;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );

my $salt = 38473827;

my $db = new DBM::Deep(
	file => $filename,
);

##
# Set digest handler
##
$db->_get_self->{engine}->set_digest( \&my_digest, 8 );

##
# put/get key
##
$db->{key1} = "value1";
ok( $db->{key1} eq "value1" );

$db->put("key2", "value2");
ok( $db->get("key2") eq "value2" );

##
# key exists
##
ok( $db->exists("key1") );
ok( exists $db->{key2} );

##
# count keys
##
ok( scalar keys %$db == 2 );

##
# step through keys
##
my $temphash = {};
while ( my ($key, $value) = each %$db ) {
	$temphash->{$key} = $value;
}

ok( ($temphash->{key1} eq "value1") && ($temphash->{key2} eq "value2") );

$temphash = {};
my $key = $db->first_key();
while ($key) {
	$temphash->{$key} = $db->get($key);
	$key = $db->next_key($key);
}

ok( ($temphash->{key1} eq "value1") && ($temphash->{key2} eq "value2") );

##
# delete keys
##
ok( delete $db->{key1} );
ok( $db->delete("key2") );

ok( scalar keys %$db == 0 );

##
# delete all keys
##
$db->put("another", "value");
$db->clear();

ok( scalar keys %$db == 0 );

##
# replace key
##
$db->put("key1", "value1");
$db->put("key1", "value2");

ok( $db->get("key1") eq "value2" );

$db->put("key1", "value222222222222222222222222");

ok( $db->get("key1") eq "value222222222222222222222222" );

##
# close, delete file, exit
##
undef $db;
unlink "t/test.db";
exit(0);

sub my_digest {
	##
	# Warning: This digest function is for testing ONLY
	# It is NOT intended for actual use
	##
	my $key = shift;
	my $num = $salt;
	
	for (my $k=0; $k<length($key); $k++) {
		$num += ord( substr($key, $k, 1) );
	}
	
	return sprintf("%00000008d", $num);
}
