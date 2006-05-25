##
# DBM::Deep Test
##
use strict;
use Test::More;

plan skip_all => "You must set \$ENV{LONG_TESTS} to run the long tests"
    unless $ENV{LONG_TESTS};

plan tests => 4;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

diag "This test can take up to a minute to run. Please be patient.";

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
	type => DBM::Deep->TYPE_ARRAY,
);

##
# put/get many keys
##
my $max_keys = 4000;

for ( 0 .. $max_keys ) {
    $db->put( $_ => $_ * 2 );
}

my $count = -1;
for ( 0 .. $max_keys ) {
    $count = $_;
    unless ( $db->get( $_ ) == $_ * 2 ) {
        last;
    };
}
is( $count, $max_keys, "We read $count keys" );

cmp_ok( scalar(@$db), '==', $max_keys + 1, "Number of elements is correct" );
$db->clear;
cmp_ok( scalar(@$db), '==', 0, "Number of elements after clear() is correct" );
