##
# DBM::Deep Test
##
use strict;
use Test::More tests => 12;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new( "t/test.db" );
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# Create structure in $db
##
$db->import(
	hash1 => {
		subkey1 => "subvalue1",
		subkey2 => "subvalue2",
	},
    hash2 => {
        subkey3 => 'subvalue3',
    },
);

is( $db->{hash1}{subkey1}, 'subvalue1', "Value imported correctly" );
is( $db->{hash1}{subkey2}, 'subvalue2', "Value imported correctly" );

$db->{copy} = $db->{hash1};

is( $db->{copy}{subkey1}, 'subvalue1', "Value copied correctly" );
is( $db->{copy}{subkey2}, 'subvalue2', "Value copied correctly" );

$db->{copy}{subkey1} = "another value";
is( $db->{copy}{subkey1}, 'another value', "New value is set correctly" );
is( $db->{hash1}{subkey1}, 'another value', "Old value is set to the new one" );

is( scalar(keys %{$db->{hash1}}), 2, "Start with 2 keys in the original" );
is( scalar(keys %{$db->{copy}}), 2, "Start with 2 keys in the copy" );

delete $db->{copy}{subkey2};

is( scalar(keys %{$db->{copy}}), 1, "Now only have 1 key in the copy" );
is( scalar(keys %{$db->{hash1}}), 1, "... and only 1 key in the original" );

$db->{copy} = $db->{hash2};
is( $db->{copy}{subkey3}, 'subvalue3', "After the second copy, we're still good" );
