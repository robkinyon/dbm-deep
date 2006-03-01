##
# DBM::Deep Test
##
use strict;
use Test::More tests => 4;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
my $db = DBM::Deep->new(
	file => $filename,
	locking => 1,
);

##
# basic put/get
##
$db->{key1} = "value1";
is( $db->{key1}, "value1", "key1 is set" );

$db->{key2} = [ 1 .. 3 ];
is( $db->{key2}[1], 2 );

##
# explicit lock
##
$db->lock( DBM::Deep->LOCK_EX );
$db->{key1} = "value2";
$db->unlock();
is( $db->{key1}, "value2", "key1 is overridden" );
