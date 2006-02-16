##
# DBM::Deep Test
##
use strict;
use Test::More tests => 5;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new( "t/test.db" );
if ($db->error()) {
	die "ERROR: " . $db->error();
}

unlink "t/test2.db";
my $db2 = DBM::Deep->new( "t/test2.db" );
if ($db2->error()) {
	die "ERROR: " . $db2->error();
}

##
# Create structure in $db
##
$db->import(
	hash1 => {
		subkey1 => "subvalue1",
		subkey2 => "subvalue2"
	}
);

is( $db->{hash1}{subkey1}, 'subvalue1', "Value imported correctly" );
is( $db->{hash1}{subkey2}, 'subvalue2', "Value imported correctly" );

##
# Cross-ref nested hash accross DB objects
##
$db2->{copy} = $db->{hash1};

##
# close, delete $db
##
undef $db;
unlink "t/test.db";

##
# Make sure $db2 has copy of $db's hash structure
##
is( $db2->{copy}{subkey1}, 'subvalue1', "Value copied correctly" );
is( $db2->{copy}{subkey2}, 'subvalue2', "Value copied correctly" );
