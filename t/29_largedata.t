##
# DBM::Deep Test
##
use strict;
use Test::More tests => 4;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
);

##
# large keys
##
my $val1 = "a" x 1000;

$db->{foo} = $val1;
is( $db->{foo}, $val1, "1000 char value stored and retrieved" );

delete $db->{foo};
my $size = -s $filename;
$db->{bar} = "a" x 300;
is( $db->{bar}, 'a' x 300, "New 256 char value is stored" );
cmp_ok( $size, '==', -s $filename, "Freespace is reused" );
