use strict;

use Test::More tests => 10;
use Test::Exception;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );

my $x = 25;
{
    my $db = DBM::Deep->new( $filename );

    throws_ok {
        $db->{scalarref} = \$x;
    } qr/Storage of variables of type 'SCALAR' is not supported/,
    'Storage of scalar refs not supported';

    throws_ok {
        $db->{scalarref} = \\$x;
    } qr/Storage of variables of type 'REF' is not supported/,
    'Storage of ref refs not supported';

    throws_ok {
        $db->{scalarref} = sub { 1 };
    } qr/Storage of variables of type 'CODE' is not supported/,
    'Storage of code refs not supported';

    throws_ok {
        $db->{scalarref} = $db->_get_self->_fh;
    } qr/Storage of variables of type 'GLOB' is not supported/,
    'Storage of glob refs not supported';

    $db->{scalar} = $x;
    TODO: {
        todo_skip "Refs to DBM::Deep objects aren't implemented yet", 2;
        lives_ok {
            $db->{selfref} = \$db->{scalar};
        } "Refs to DBM::Deep objects are ok";

        is( ${$db->{selfref}}, $x, "A ref to a DBM::Deep object is ok" );
    }
}

{
    my $db = DBM::Deep->new( $filename );

    is( $db->{scalar}, $x, "Scalar retrieved ok" );
    TODO: {
        todo_skip "Refs to DBM::Deep objects aren't implemented yet", 2;
        is( ${$db->{scalarref}}, 30, "Scalarref retrieved ok" );
        is( ${$db->{selfref}}, 26, "Scalarref to stored scalar retrieved ok" );
    }
}
