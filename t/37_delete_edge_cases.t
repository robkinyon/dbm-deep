##
# DBM::Deep Test
##
use strict;
use Test::More tests => 5;
use Test::Deep;
use Clone::Any qw( clone );
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

ok( !tied(%$x), "\$x is NOT tied" );
cmp_deeply( $x, $x_save, "When it's deleted, it's untied" );
