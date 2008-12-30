use 5.006_000;

use strict;
use warnings FATAL => 'all';

use Test::More;

use t::common qw( new_fh );

my $max = 10;

plan tests => $max + 1;

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( file => $filename, fh => $fh, );

my $x = 1;
while( $x <= $max ) {
    eval {
        delete $db->{borked}{test};
        $db->{borked}{test} = 1;
    };

    ok(!$@, "No eval failure after ${x}th iteration");
    $x++;
}
