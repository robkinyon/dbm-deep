##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;

use_ok( 'DBM::Deep' );

my ($before, $after);

{
    unlink "t/test.db";
    my $db = DBM::Deep->new(
        file => "t/test.db",
        autoflush => 1
    );
    $db->{key1} = "value1";
    $db->{key2} = "value2";
    $before = (stat($db->_fh()))[7];
}

{
    unlink "t/test.db";
    my $db = DBM::Deep->new(
        file => "t/test.db",
        autoflush => 1
    );

    ##
    # set pack to 2-byte (16-bit) words
    ##
    $db->_get_self->{engine}->set_pack( 2, 'S' );

    $db->{key1} = "value1";
    $db->{key2} = "value2";
    $after = (stat($db->_fh()))[7];
}

ok( $after < $before, "The new packsize reduced the size of the file" );
