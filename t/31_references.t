##
# DBM::Deep Test
##
use strict;
use Test::More tests => 6;
use Test::Exception;
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
flock $fh, LOCK_UN;
my $db = DBM::Deep->new( $filename );

my %hash = (
    foo => 1,
    bar => [ 1 .. 3 ],
    baz => { a => 42 },
);

$db->{hash} = \%hash;
isa_ok( tied(%hash), 'DBM::Deep::Hash' );

is( $db->{hash}{foo}, 1 );
is_deeply( $db->{hash}{bar}, [ 1 .. 3 ] );
is_deeply( $db->{hash}{baz}, { a => 42 } );

$hash{foo} = 2;
is( $db->{hash}{foo}, 2 );
