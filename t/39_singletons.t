use strict;
use Test::More tests => 2;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
);

$db->{foo} = { a => 'b' };
my $x = $db->{foo};
my $y = $db->{foo};

print "$x -> $y\n";

is( $x, $y, "The references are the same" );
