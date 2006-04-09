use strict;
use Test::More tests => 4;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
);

$db->{x} = 'y';
is( $db->{x}, 'y' );
$db->begin_work;
$db->{x} = 'z';
is( $db->{x}, 'z' );
$db->rollback;
is( $db->{x}, 'y' );

# Add a commit test using fork
