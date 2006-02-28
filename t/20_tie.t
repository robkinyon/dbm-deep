##
# DBM::Deep Test
##
use strict;
use Test::More tests => 11;
use Test::Exception;

use_ok( 'DBM::Deep' );

##
# testing the various modes of opening a file
##
{
    unlink "t/test.db";
    my %hash;
    my $db = tie %hash, 'DBM::Deep', 't/test.db';

    ok(1, "Tied an hash with an array for params" );
}

{
    unlink "t/test.db";
    my %hash;
    my $db = tie %hash, 'DBM::Deep', {
        file => 't/test.db',
    };

    ok(1, "Tied a hash with a hashref for params" );
}

{
    unlink "t/test.db";
    my @array;
    my $db = tie @array, 'DBM::Deep', 't/test.db';

    ok(1, "Tied an array with an array for params" );

    is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
}

{
    unlink "t/test.db";
    my @array;
    my $db = tie @array, 'DBM::Deep', {
        file => 't/test.db',
    };

    ok(1, "Tied an array with a hashref for params" );

    is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
}

unlink "t/test.db";
throws_ok {
    tie my %hash, 'DBM::Deep', [ file => 't/test.db' ];
} qr/Not a hashref/, "Passing an arrayref to TIEHASH fails";

unlink "t/test.db";
throws_ok {
    tie my @array, 'DBM::Deep', [ file => 't/test.db' ];
} qr/Not a hashref/, "Passing an arrayref to TIEARRAY fails";

unlink "t/test.db";
throws_ok {
    tie my %hash, 'DBM::Deep', undef, file => 't/test.db';
} qr/Odd number of parameters/, "Odd number of params to TIEHASH fails";

unlink "t/test.db";
throws_ok {
    tie my @array, 'DBM::Deep', undef, file => 't/test.db';
} qr/Odd number of parameters/, "Odd number of params to TIEARRAY fails";
