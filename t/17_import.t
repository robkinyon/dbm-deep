##
# DBM::Deep Test
##
use strict;
use Test::More tests => 6;
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
    noclass({
        key1 => 'value1',
        key2 => 'value2',
        array1 => [ 'elem0', 'elem1', 'elem2', ],
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
            subkey3 => useclass( bless {}, 'Foo' ),
        },
    }),
    "Everything matches",
);

$struct->{foo} = 'bar';
is( $struct->{foo}, 'bar', "\$struct has foo and it's 'bar'" );
ok( !exists $db->{foo}, "\$db doesn't have the 'foo' key, so \$struct is not tied" );

$struct->{hash1}->{foo} = 'bar';
is( $struct->{hash1}->{foo}, 'bar', "\$struct->{hash1} has foo and it's 'bar'" );
ok( !exists $db->{hash1}->{foo}, "\$db->{hash1} doesn't have the 'foo' key, so \$struct->{hash1} is not tied" );
