
use strict;
use Test::More;

use t::common qw( new_fh );

my $max = 10;

plan skip_all => "Need to work on this one later.";

plan tests => $max + 1;

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( file => $filename, fh => $fh, );

my $x = 0;
while( $x < $max ) {
    eval {
        delete $db->{borked}{test};
        $db->{borked}{test} = 1;
    };

    ok(!$@, 'No eval failures');
    $x++;
}
