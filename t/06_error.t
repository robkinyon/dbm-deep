##
# DBM::Deep Test
##
$|++;
use strict;
use Test::More tests => 6;
use Test::Exception;

use_ok( 'DBM::Deep' );

##
# make sure you can clear the error state
##
##
# test a corrupted file
##
open FH, '>t/test.db';
print FH 'DPDB';
close FH;
throws_ok {
    DBM::Deep->new( "t/test.db" );
} qr/DBM::Deep: Corrupted file, no master index record/, "Fail if there's no master index record";

{
    unlink "t/test.db";
    my %hash;
    tie %hash, 'DBM::Deep', 't/test.db';
    undef %hash;

    my @array;
    throws_ok {
        tie @array, 'DBM::Deep', 't/test.db';
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and tie a hash file with an array";

    throws_ok {
        DBM::Deep->new( file => 't/test.db', type => DBM::Deep->TYPE_ARRAY )
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and open a hash file with an array";
}

{
    unlink "t/test.db";
    my @array;
    tie @array, 'DBM::Deep', 't/test.db';
    undef @array;

    my %hash;
    throws_ok {
        tie %hash, 'DBM::Deep', 't/test.db';
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and tie an array file with a hash";

    throws_ok {
        DBM::Deep->new( file => 't/test.db', type => DBM::Deep->TYPE_HASH )
    } qr/DBM::Deep: File type mismatch/, "Fail if we try and open an array file with a hash";
}
