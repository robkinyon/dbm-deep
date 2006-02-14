##
# DBM::Deep Test
##
use strict;
use Test::More tests => 4;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new( "t/test.db" );
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# cause an error
##
eval { $db->push("foo"); }; # ERROR -- array-only method

ok( $db->error() );

$db->clear_error();

ok( !$db->error() );
undef $db;

open FH, '>t/test.db';
print FH 'DPDB';
close FH;
$db = DBM::Deep->new( "t/test.db" );
TODO: {
    local $TODO = "The return value from load_tag() isn't checked in open()";
    ok( $db->error() );
}
