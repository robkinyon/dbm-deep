##
# DBM::Deep Test
##
use strict;
use Test::More;

my $max_levels = 1000;

plan tests => 5;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# basic deep hash
##
$db->{company} = {};
$db->{company}->{name} = "My Co.";
$db->{company}->{employees} = {};
$db->{company}->{employees}->{"Henry Higgins"} = {};
$db->{company}->{employees}->{"Henry Higgins"}->{salary} = 90000;

is( $db->{company}->{name}, "My Co.", "Set and retrieved a second-level value" );
is( $db->{company}->{employees}->{"Henry Higgins"}->{salary}, 90000, "Set and retrieved a fourth-level value" );

##
# super deep hash
##
$db->{base_level} = {};
my $temp_db = $db->{base_level};

for my $k ( 0 .. $max_levels ) {
	$temp_db->{"level$k"} = {};
	$temp_db = $temp_db->{"level$k"};
}
$temp_db->{deepkey} = "deepvalue";
undef $temp_db;

undef $db;
$db = DBM::Deep->new(
	file => "t/test.db",
	type => DBM::Deep->TYPE_HASH,
);

my $cur_level = -1;
$temp_db = $db->{base_level};
for my $k ( 0 .. $max_levels ) {
    $cur_level = $k;
    $temp_db = $temp_db->{"level$k"};
    eval { $temp_db->isa( 'DBM::Deep' ) } or last;
}
is( $cur_level, $max_levels, "We read all the way down to level $cur_level" );
is( $temp_db->{deepkey}, "deepvalue", "And we retrieved the value at the bottom of the ocean" );
