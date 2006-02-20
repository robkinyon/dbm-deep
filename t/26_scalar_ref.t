use strict;

use Test::More tests => 7;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
{
    my $db = DBM::Deep->new( "t/test.db" );
    if ($db->error()) {
        die "ERROR: " . $db->error();
    }

    my $x = 25;
    my $y = 30;
    $db->{scalar} = $x;
    $db->{scalarref} = \$y;
    $db->{selfref} = \$x;

    is( $db->{scalar}, $x, "Scalar retrieved ok" );
    TODO: {
        todo_skip "Scalar refs aren't implemented yet", 2;
        is( ${$db->{scalarref}}, 30, "Scalarref retrieved ok" );
        is( ${$db->{selfref}}, 25, "Scalarref to stored scalar retrieved ok" );
    }
}

{
    my $db = DBM::Deep->new( "t/test.db" );
    if ($db->error()) {
        die "ERROR: " . $db->error();
    }

    my $x = 25;
    my $y = 30;
    is( $db->{scalar}, $x, "Scalar retrieved ok" );
    TODO: {
        todo_skip "Scalar refs aren't implemented yet", 2;
        is( ${$db->{scalarref}}, 30, "Scalarref retrieved ok" );
        is( ${$db->{selfref}}, 25, "Scalarref to stored scalar retrieved ok" );
    }
}
