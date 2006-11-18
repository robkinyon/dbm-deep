use strict;
use Test::More tests => 7;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db1 = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
);

my $x_outer = { a => 'b' };
my $x_inner = { a => 'c' };

$db1->{x} = $x_outer;
is( $db1->{x}{a}, 'b', "BEFORE: We're looking at the right value from outer" );

$db1->begin_work;

    $db1->{x} = $x_inner;
    is( $db1->{x}{a}, 'c', "WITHIN: We're looking at the right value from inner" );
TODO: {
    local $TODO = "Transactions not done yet";
    is( $x_outer->{a}, 'c', "WITHIN: We're looking at the right value from outer" );
}

$db1->commit;

is( $db1->{x}{a}, 'c', "AFTER: Commit means x_inner is still correct" );
TODO: {
    local $TODO = "Transactions not done yet";
is( $x_outer->{a}, 'c', "AFTER: outer made the move" );
is( $x_inner->{a}, 'c', "AFTER: inner made the move" );
}
