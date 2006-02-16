##
# DBM::Deep Test
##
use strict;
use Test::More;
use Test::Exception;

plan tests => 5;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new( "t/test.db" );
if ($db->error()) {
	die "ERROR: " . $db->error();
}

$db->{key1} = "value1";
is( $db->{key1}, "value1", "Value set correctly" );

# Testing to verify that the close() will occur if open is called on an open DB.
$db->_open;

is( $db->{key1}, "value1", "Value still set after re-open" );

throws_ok {
    my $db = DBM::Deep->new( 't' );
} qr/^DBM::Deep: Cannot open file: t: /, "Can't open a file we aren't allowed to touch";

throws_ok {
    my $db = DBM::Deep->new( __FILE__ );
} qr/^DBM::Deep: Signature not found -- file is not a Deep DB/, "Only DBM::Deep DB files will be opened";
