##
# DBM::Deep Test
##
use strict;
use Test::More;

my $max_keys = 4000;
plan tests => 2 + $max_keys;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
	type => DBM::Deep->TYPE_ARRAY
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# put/get many keys
##
for ( 0 .. $max_keys ) {
    $db->put( $_ => $_ * 2 );
}

for ( 0 .. $max_keys ) {
    is( $db->get( $_ ), $_ * 2, "The ${_}th value is correct" );
}
