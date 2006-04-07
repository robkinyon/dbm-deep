##
# DBM::Deep Test
##
$|++;
use strict;
use Test::More tests => 6;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();

##
# test a corrupted file
##
open FH, ">$filename";
print FH 'DPDB';
close FH;
throws_ok {
    DBM::Deep->new( $filename );
} qr/DBM::Deep: Corrupted file - bad header/, "Fail if there's a bad header";

{
    my ($fh, $filename) = new_fh();
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
    my ($fh, $filename) = new_fh();
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
