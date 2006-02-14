##
# DBM::Deep Test
##
use strict;
use Test::More;

my $max_levels = 1000;

plan tests => $max_levels + 5;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db"
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
	file => "t/test.db"
);

$temp_db = $db->{base_level};
for my $k ( 0 .. $max_levels ) {
    $temp_db = $temp_db->{"level$k"};
    isa_ok( $temp_db, 'DBM::Deep' ) || die "Whoops!";
}
is( $temp_db->{deepkey}, "deepvalue", "And we retrieved the value at the bottom of the ocean" );
