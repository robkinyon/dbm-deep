use strict;

use Test::More tests => 10;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();

my $x = 25;
{
    my $db = DBM::Deep->new( $filename );

    throws_ok {
        $db->{scalarref} = \$x;
    } qr/Storage of references of type 'SCALAR' is not supported/,
    'Storage of scalar refs not supported';

    throws_ok {
        $db->{scalarref} = \\$x;
    } qr/Storage of references of type 'REF' is not supported/,
    'Storage of ref refs not supported';

    throws_ok {
        $db->{scalarref} = sub { 1 };
    } qr/Storage of references of type 'CODE' is not supported/,
    'Storage of code refs not supported';

    throws_ok {
        $db->{scalarref} = $fh;
    } qr/Storage of references of type 'GLOB' is not supported/,
    'Storage of glob refs not supported';

    warn "\n1: " . $db->_engine->_dump_file;
    $db->{scalar} = $x;
    warn "\n2: " . $db->_engine->_dump_file;
    TODO: {
        todo_skip "Refs to DBM::Deep objects aren't implemented yet", 2;
        lives_ok {
            $db->{selfref} = \$db->{scalar};
        } "Refs to DBM::Deep objects are ok";

        is( ${$db->{selfref}}, $x, "A ref to a DBM::Deep object is ok" );
    }

    warn $db->_engine->_dump_file;
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
