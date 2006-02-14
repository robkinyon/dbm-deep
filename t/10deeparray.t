##
# DBM::Deep Test
##
use strict;
use Test::More;

my $max_levels = 1000;

plan tests => $max_levels + 3;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
	type => DBM::Deep->TYPE_ARRAY,
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

$db->[0] = [];
my $temp_db = $db->[0];
for my $k ( 0 .. $max_levels ) {
	$temp_db->[$k] = [];
	$temp_db = $temp_db->[$k];
}
$temp_db->[0] = "deepvalue";
undef $temp_db;

undef $db;
$db = DBM::Deep->new(
	file => "t/test.db",
	type => DBM::Deep->TYPE_ARRAY,
);

$temp_db = $db->[0];
for my $k ( 0 .. $max_levels ) {
    $temp_db = $temp_db->[$k];
    isa_ok( $temp_db, 'DBM::Deep' ) || die "Whoops!";
}
is( $temp_db->[0], "deepvalue", "And we retrieved the value at the bottom of the ocean" );
