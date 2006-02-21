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

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1, "Tied an hash with an array for params" ); }
}

{
    unlink "t/test.db";
    my %hash;
    my $db = tie %hash, 'DBM::Deep', {
        file => 't/test.db',
    };

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1, "Tied a hash with a hashref for params" ); }
}

{
    unlink "t/test.db";
    my @array;
    my $db = tie @array, 'DBM::Deep', 't/test.db';

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1, "Tied an array with an array for params" ); }

    is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
}

{
    unlink "t/test.db";
    my @array;
    my $db = tie @array, 'DBM::Deep', {
        file => 't/test.db',
    };

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1, "Tied an array with a hashref for params" ); }

    is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
}

# These are testing the naive use of ref() within TIEHASH and TIEARRAY.
# They should be doing (Scalar::Util::reftype($_[0]) eq 'HASH') and then
# erroring out if it's not.
TODO: {
    todo_skip( "Naive use of {\@_}", 1 );
    unlink "t/test.db";
    my %hash;
    my $db = tie %hash, 'DBM::Deep', [
        file => 't/test.db',
    ];

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1); }
}

TODO: {
    todo_skip( "Naive use of {\@_}", 1 );
    unlink "t/test.db";
    my @array;
    my $db = tie @array, 'DBM::Deep', [
        file => 't/test.db',
    ];

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1); }
}

unlink "t/test.db";
throws_ok {
    tie my %hash, 'DBM::Deep', undef, file => 't/test.db';
} qr/Odd number of parameters/, "Odd number of params to TIEHASH fails";

unlink "t/test.db";
throws_ok {
    tie my @array, 'DBM::Deep', undef, file => 't/test.db';
} qr/Odd number of parameters/, "Odd number of params to TIEARRAY fails";
