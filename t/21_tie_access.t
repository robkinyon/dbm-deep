##
# DBM::Deep Test
##
use strict;
use Test::More;
use Test::Exception;

plan tests => 7;

use_ok( 'DBM::Deep' );

# How should one test for creation failure with the tie mechanism?

unlink "t/test.db";

{
    my %hash;
    tie %hash, 'DBM::Deep', "t/test.db";

    $hash{key1} = 'value';
    is( $hash{key1}, 'value', 'Set and retrieved key1' );
}

{
    my %hash;
    tie %hash, 'DBM::Deep', "t/test.db";

    is( $hash{key1}, 'value', 'Set and retrieved key1' );

    is( keys %hash, 1, "There's one key so far" );
    ok( exists $hash{key1}, "... and it's key1" );
}

throws_ok {
    tie my @array, 'DBM::Deep', {
        file => 't/test.db',
        type => DBM::Deep->TYPE_ARRAY,
    };
} qr/DBM::Deep: File type mismatch/, "\$SIG_TYPE doesn't match file's type";

unlink "t/test.db";
DBM::Deep->new( file => 't/test.db', type => DBM::Deep->TYPE_ARRAY );

throws_ok {
    tie my %hash, 'DBM::Deep', {
        file => 't/test.db',
        type => DBM::Deep->TYPE_HASH,
    };
} qr/DBM::Deep: File type mismatch/, "\$SIG_TYPE doesn't match file's type";
