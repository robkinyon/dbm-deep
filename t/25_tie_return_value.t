use strict;

use Test::More tests => 5;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );

use Scalar::Util qw( reftype );

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );

    my %hash;
    my $obj = tie %hash, 'DBM::Deep', $filename;
    isa_ok( $obj, 'DBM::Deep' );
    is( reftype( $obj ), 'HASH', "... and its underlying representation is an HASH" );
}

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );

    my @array;
    my $obj = tie @array, 'DBM::Deep', $filename;
    isa_ok( $obj, 'DBM::Deep' );
    is( reftype( $obj ), 'HASH', "... and its underlying representation is an HASH" );
}
