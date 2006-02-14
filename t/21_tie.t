##
# DBM::Deep Test
##
use strict;
use Test::More;
BEGIN { plan tests => 10 }

use DBM::Deep;

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
    else { ok(1); }
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
    else { ok(1); }
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
    else { ok(1); }

    TODO: {
        local $TODO = "TIE_ARRAY doesn't set the type correctly";
        is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
    }
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
    else { ok(1); }

    TODO: {
        local $TODO = "TIE_ARRAY doesn't set the type correctly";
        is( $db->{type}, DBM::Deep->TYPE_ARRAY, "TIE_ARRAY sets the correct type" );
    }
}

# These are testing the naive use of ref() within TIEHASH and TIEARRAY.
# They should be doing (Scalar::Util::reftype($_[0]) eq 'HASH') and then
# erroring out if it's not.
TODO: {
    todo_skip "Naive use of ref()", 1;
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
    todo_skip "Naive use of ref()", 1;
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

# These are testing the naive use of the {@_} construct within TIEHASH and
# TIEARRAY. Instead, they should be checking (@_ % 2 == 0) and erroring out
# if it's not.
TODO: {
    todo_skip( "Naive use of {\@_}", 1 );
    unlink "t/test.db";
    my %hash;
    my $db = tie %hash, 'DBM::Deep',
        undef, file => 't/test.db'
    ;

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
    my $db = tie @array, 'DBM::Deep',
        undef, file => 't/test.db'
    ;

    if ($db->error()) {
        print "ERROR: " . $db->error();
        ok(0);
        exit(0);
    }
    else { ok(1); }
}
