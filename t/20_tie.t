##
# DBM::Deep Test
##
use strict;
use Test::More tests => 11;
use Test::Exception;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );

##
# testing the various modes of opening a file
##
{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my %hash;
    my $db = tie %hash, 'DBM::Deep', $filename;

    ok(1, "Tied an hash with an array for params" );
}

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my %hash;
    my $db = tie %hash, 'DBM::Deep', {
        file => $filename,
    };

    ok(1, "Tied a hash with a hashref for params" );
}

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my @array;
    my $db = tie @array, 'DBM::Deep', $filename;

    ok(1, "Tied an array with an array for params" );

    is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
}

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my @array;
    my $db = tie @array, 'DBM::Deep', {
        file => $filename,
    };

    ok(1, "Tied an array with a hashref for params" );

    is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
}

my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
throws_ok {
    tie my %hash, 'DBM::Deep', [ file => $filename ];
} qr/Not a hashref/, "Passing an arrayref to TIEHASH fails";

unlink "t/test.db";
throws_ok {
    tie my @array, 'DBM::Deep', [ file => $filename ];
} qr/Not a hashref/, "Passing an arrayref to TIEARRAY fails";

unlink "t/test.db";
throws_ok {
    tie my %hash, 'DBM::Deep', undef, file => $filename;
} qr/Odd number of parameters/, "Odd number of params to TIEHASH fails";

unlink "t/test.db";
throws_ok {
    tie my @array, 'DBM::Deep', undef, file => $filename;
} qr/Odd number of parameters/, "Odd number of params to TIEARRAY fails";
