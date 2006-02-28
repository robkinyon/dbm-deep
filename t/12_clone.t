##
# DBM::Deep Test
##
use strict;
use Test::More tests => 14;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new(
	file => "t/test.db",
);

$db->{key1} = "value1";

##
# clone db handle, make sure both are usable
##
my $clone = $db->clone();

is($clone->{key1}, "value1");

$clone->{key2} = "value2";
$db->{key3} = "value3";

is($db->{key1}, "value1");
is($db->{key2}, "value2");
is($db->{key3}, "value3");

is($clone->{key1}, "value1");
is($clone->{key2}, "value2");
is($clone->{key3}, "value3");

undef $db;

is($clone->{key1}, "value1");
is($clone->{key2}, "value2");
is($clone->{key3}, "value3");

undef $clone;

$db = DBM::Deep->new(
	file => "t/test.db",
);

is($db->{key1}, "value1");
is($db->{key2}, "value2");
is($db->{key3}, "value3");
