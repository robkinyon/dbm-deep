##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new( "t/test.db" );

##
# Create structure in memory
##
my $struct = {
	key1 => "value1",
	key2 => "value2",
	array1 => [ "elem0", "elem1", "elem2" ],
	hash1 => {
		subkey1 => "subvalue1",
		subkey2 => "subvalue2"
	}
};

##
# Import entire thing
##
$db->import( $struct );
undef $struct;

##
# Make sure everything is there
##
ok(
	($db->{key1} eq "value1") && 
	($db->{key2} eq "value2") && 
	($db->{array1} && 
		($db->{array1}->[0] eq "elem0") &&
		($db->{array1}->[1] eq "elem1") && 
		($db->{array1}->[2] eq "elem2")
	) && 
	($db->{hash1} &&
		($db->{hash1}->{subkey1} eq "subvalue1") && 
		($db->{hash1}->{subkey2} eq "subvalue2")
	)
);
