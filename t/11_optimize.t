##
# DBM::Deep Test
##
use strict;
use Test::More tests => 5;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
	autoflush => 1,
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# create some unused space
##
$db->{key1} = "value1";
$db->{key2} = "value2";

$db->{a} = {};
$db->{a}{b} = [];
$db->{a}{c} = 'value2';

my $b = $db->{a}->{b};
$b->[0] = 1;
$b->[1] = 2;
$b->[2] = {};
$b->[2]->{c} = [];

my $c = $b->[2]->{c};
$c->[0] = 'd';
$c->[1] = {};
$c->[1]->{e} = 'f';

undef $c;
undef $b;

delete $db->{key2};
delete $db->{a}{b};

##
# take byte count readings before, and after optimize
##
my $before = (stat($db->fh()))[7];
my $result = $db->optimize();
my $after = (stat($db->fh()))[7];

if ($db->error()) {
	die "ERROR: " . $db->error();
}

ok( $result );
ok( $after < $before ); # make sure file shrunk

is( $db->{key1}, 'value1', "key1's value is still there after optimize" );
is( $db->{a}{c}, 'value2', "key2's value is still there after optimize" );
