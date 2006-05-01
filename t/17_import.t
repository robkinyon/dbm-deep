##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new({
    file      => $filename,
    autobless => 1,
});

##
# Create structure in memory
##
my $struct = {
	key1 => "value1",
	key2 => "value2",
	array1 => [ "elem0", "elem1", "elem2" ],
	hash1 => {
		subkey1 => "subvalue1",
		subkey2 => "subvalue2",
        subkey3 => bless( {}, 'Foo' ),
	}
};

##
# Import entire thing
##
$db->import( $struct );

cmp_deeply(
    $db,
    {
        key1 => 'value1',
        key2 => 'value2',
        array1 => [ 'elem0', 'elem1', 'elem2', ],
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
            subkey3 => useclass( bless {}, 'Foo' ),
        },
    },
    "Everything matches",
);
