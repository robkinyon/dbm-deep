##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
	type => DBM::Deep->TYPE_HASH
);

##
# put/get many keys
##
my $max_keys = 4000;

for ( 0 .. $max_keys ) {
    $db->put( "hello $_" => "there " . $_ * 2 );
}

my $count = -1;
for ( 0 .. $max_keys ) {
    $count = $_;
    unless ( $db->get( "hello $_" ) eq "there " . $_ * 2 ) {
        last;
    };
}
is( $count, $max_keys, "We read $count keys" );
