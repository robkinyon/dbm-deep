##
# DBM::Deep Test
##
use strict;
use Test;
BEGIN { plan tests => 2 }

use DBM::Deep;

##
# basic file open
##
unlink "t/test.db";
my $db = new DBM::Deep(
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
ok( $db->{key1} eq "value1" );

##
# explicit lock
##
$db->lock( DBM::Deep::LOCK_EX );
$db->{key1} = "value2";
$db->unlock();
ok( $db->{key1} eq "value2" );

##
# close, delete file, exit
##
undef $db;
unlink "t/test.db";
exit(0);
