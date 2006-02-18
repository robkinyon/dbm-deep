use strict;

use Test::More tests => 5;

use Scalar::Util qw( reftype );

use_ok( 'DBM::Deep' );

{
    unlink "t/test.db";

    my %hash;
    my $obj = tie %hash, 'DBM::Deep', 't/test.db';
    isa_ok( $obj, 'DBM::Deep' );
    is( reftype( $obj ), 'HASH', "... and its underlying representation is an HASH" );
}

{
    unlink "t/test.db";

    my @array;
    my $obj = tie @array, 'DBM::Deep', 't/test.db';
    isa_ok( $obj, 'DBM::Deep' );
    TODO: {
        local $TODO = "_init() returns a blessed hashref";
        is( reftype( $obj ), 'ARRAY', "... and its underlying representation is an ARRAY" );
    }
}
