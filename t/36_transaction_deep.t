use strict;
use Test::More tests => 3;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db1 = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
);

$db1->begin_work;

    my $x = { a => 'b' };;
    $db1->{x} = $x;

$db1->commit;

is( $db1->{x}{a}, 'b', "DB1 X-A is good" );
is( $x->{a}, 'b', "X's A is good" );
