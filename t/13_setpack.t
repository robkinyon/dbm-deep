##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
	autoflush => 1
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}
$db->{key1} = "value1";
$db->{key2} = "value2";
my $before = (stat($db->_fh()))[7];
undef $db;

##
# set pack to 2-byte (16-bit) words
##
DBM::Deep::set_pack(2, 'S');

unlink "t/test.db";
$db = DBM::Deep->new(
	file => "t/test.db",
	autoflush => 1
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}
$db->{key1} = "value1";
$db->{key2} = "value2";
my $after = (stat($db->_fh()))[7];
undef $db;

ok( $after < $before );
