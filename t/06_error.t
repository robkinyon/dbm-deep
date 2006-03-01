##
# DBM::Deep Test
##
$|++;
use strict;
use Test::More tests => 6;
use Test::Exception;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );

##
# test a corrupted file
##
open FH, ">$filename";
print FH 'DPDB';
close FH;
throws_ok {
    DBM::Deep->new( $filename );
} qr/DBM::Deep: Corrupted file, no master index record/, "Fail if there's no master index record";

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my %hash;
    tie %hash, 'DBM::Deep', $filename;
    undef %hash;

    my @array;
    throws_ok {
        tie @array, 'DBM::Deep', $filename;
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and tie a hash file with an array";

    throws_ok {
        DBM::Deep->new( file => $filename, type => DBM::Deep->TYPE_ARRAY )
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and open a hash file with an array";
}

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my @array;
    tie @array, 'DBM::Deep', $filename;
    undef @array;

    my %hash;
    throws_ok {
        tie %hash, 'DBM::Deep', $filename;
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and tie an array file with a hash";

    throws_ok {
        DBM::Deep->new( file => $filename, type => DBM::Deep->TYPE_HASH )
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and open an array file with a hash";
}
