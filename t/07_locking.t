##
# DBM::Deep Test
##
use strict;
use Test::More tests => 5;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
    fh => $fh,
    locking => 1,
);

lives_ok {
    $db->unlock;
} "Can call unlock on an unlocked DB.";

##
# basic put/get
##
$db->{key1} = "value1";
is( $db->{key1}, "value1", "key1 is set" );

$db->{key2} = [ 1 .. 3 ];
is( $db->{key2}[1], 2, "The value is set properly" );

##
# explicit lock
##
$db->lock_exclusive;
$db->{key1} = "value2";
$db->unlock();
is( $db->{key1}, "value2", "key1 is overridden" );
