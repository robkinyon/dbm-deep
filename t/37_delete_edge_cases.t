##
# DBM::Deep Test
##
use strict;
use Test::More tests => 4;
use Test::Deep;
use Clone qw( clone );
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( $filename );

my $x = {
    a => 1,
    b => 2,
    c => [ 1 .. 3 ],
};

my $x_save = clone( $x );

$db->{foo} = $x;
    
ok( tied(%$x), "\$x is tied" );
delete $db->{foo};

TODO: {
    local $TODO = "Delete isn't working right";
    ok( !tied(%$x), "\$x is NOT tied" );
    cmp_deeply( $x, $x_save, "When it's deleted, it's untied" );
}
