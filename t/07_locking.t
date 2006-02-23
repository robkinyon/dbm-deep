##
# DBM::Deep Test
##
use strict;
use Test::More tests => 3;

use_ok( 'DBM::Deep' );

##
# basic file open
##
unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
	locking => 1
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# basic put/get
##
$db->{key1} = "value1";
is( $db->{key1}, "value1", "key1 is set" );

##
# explicit lock
##
$db->lock( DBM::Deep->LOCK_EX );
$db->{key1} = "value2";
$db->unlock();
is( $db->{key1}, "value2", "key1 is overridden" );
