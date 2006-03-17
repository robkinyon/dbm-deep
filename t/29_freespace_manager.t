use strict;

use Test::More tests => 3;
use File::Temp qw( tempfile );
use Fcntl qw( :flock );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1 );
flock $fh, LOCK_UN;
my $db = DBM::Deep->new({
    file => $filename,
    autoflush => 1,
});

$db->{foo} = 'abcd';

my $s1 = -s $filename;

delete $db->{foo};

my $s2 = -s $filename;

is( $s2, $s1, "delete doesn't recover freespace" );

$db->{bar} = 'a';

my $s3 = -s $filename;

TODO: {
    local $TODO = "Freespace manager doesn't work yet";
    is( $s3, $s1, "Freespace is reused" );
}
