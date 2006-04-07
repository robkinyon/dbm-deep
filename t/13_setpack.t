##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($before, $after);

{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new(
        file => $filename,
        autoflush => 1,
    );
    $db->{key1} = "value1";
    $db->{key2} = "value2";
    $before = (stat($db->_fh()))[7];
}

{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new(
        file => $filename,
        autoflush => 1,
        pack_size => 'small',
    );

    $db->{key1} = "value1";
    $db->{key2} = "value2";
    $after = (stat($db->_fh()))[7];
}

ok( $after < $before, "The new packsize reduced the size of the file" );
