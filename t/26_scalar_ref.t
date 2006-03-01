use strict;

use Test::More tests => 7;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );

{
    my $db = DBM::Deep->new( $filename );

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
    my $db = DBM::Deep->new( $filename );

    my $x = 25;
    my $y = 30;
    is( $db->{scalar}, $x, "Scalar retrieved ok" );
    TODO: {
        todo_skip "Scalar refs aren't implemented yet", 2;
        is( ${$db->{scalarref}}, 30, "Scalarref retrieved ok" );
        is( ${$db->{selfref}}, 25, "Scalarref to stored scalar retrieved ok" );
    }
}
