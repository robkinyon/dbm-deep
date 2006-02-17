##
# DBM::Deep Test
##
$|++;
use strict;
use Test::More;

my $max_levels = 1000;

plan tests => 3;

use_ok( 'DBM::Deep' );
can_ok( 'DBM::Deep', 'new' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
	type => DBM::Deep->TYPE_ARRAY,
);
print "Check error( $db )\n";
if ($db->error()) {
	die "ERROR: " . $db->error();
}

print "First assignment\n";
$db->[0] = [];
print "second assignment\n";
__END__
my $temp_db = $db->[0];
print "loop\n";
for my $k ( 0 .. $max_levels ) {
	$temp_db->[$k] = [];
	$temp_db = $temp_db->[$k];
}
print "done\n";
$temp_db->[0] = "deepvalue";
print "undef\n";
undef $temp_db;

undef $db;
$db = DBM::Deep->new(
	file => "t/test.db",
	type => DBM::Deep->TYPE_ARRAY,
);

my $cur_level = -1;
$temp_db = $db->[0];
for my $k ( 0 .. $max_levels ) {
    $cur_level = $k;
    $temp_db = $temp_db->[$k];
    eval { $temp_db->isa( 'DBM::Deep' ) } or last;
}
is( $cur_level, $max_levels, "We read all the way down to level $cur_level" );
is( $temp_db->[0], "deepvalue", "And we retrieved the value at the bottom of the ocean" );
