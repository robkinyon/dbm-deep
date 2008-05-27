use strict;
use Test::More tests => 11;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new(
        file => $filename,
        locking => 1,
        autoflush => 1,
    );

    $db->{a} = 1;
    $db->{foo} = { a => 'b' };
    my $x = $db->{foo};
    my $y = $db->{foo};

    is( $x, $y, "The references are the same" );

    delete $db->{foo};
    is( $x, undef, "After deleting the DB location, external references are also undef (\$x)" );
    is( $y, undef, "After deleting the DB location, external references are also undef (\$y)" );
    is( $x + 0, undef, "DBM::Deep::Null can be added to." );
    is( $y + 0, undef, "DBM::Deep::Null can be added to." );
    is( $db->{foo}, undef, "The {foo} location is also undef." );

    # These shenanigans work to get another hashref
    # into the same data location as $db->{foo} was.
    $db->{foo} = {};
    delete $db->{foo};
    $db->{foo} = {};
    $db->{bar} = {};

    is( $x, undef, "After re-assigning to {foo}, external references to old values are still undef (\$x)" );
    is( $y, undef, "After re-assigning to {foo}, external references to old values are still undef (\$y)" );
}

SKIP: {
    skip "What do we do with external references and txns?", 2;
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new(
        file => $filename,
        locking => 1,
        autoflush => 1,
        num_txns => 2,
    );

    $db->{foo} = { a => 'b' };
    my $x = $db->{foo};

    $db->begin_work;
    
        $db->{foo} = { c => 'd' };
        my $y = $db->{foo};

        # XXX What should happen here with $x and $y?
        is( $x, $y );
        is( $x->{c}, 'd' );

    $db->rollback;
}
