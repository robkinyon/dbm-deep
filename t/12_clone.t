##
# DBM::Deep Test
##
use strict;
use Test::More tests => 14;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();

{
    my $clone;

    {
        my $db = DBM::Deep->new(
            file => $filename,
        );

        $db->{key1} = "value1";

        ##
        # clone db handle, make sure both are usable
        ##
        $clone = $db->clone();

        is($clone->{key1}, "value1");

        $clone->{key2} = "value2";
        $db->{key3} = "value3";

        is($db->{key1}, "value1");
        is($db->{key2}, "value2");
        is($db->{key3}, "value3");

        is($clone->{key1}, "value1");
        is($clone->{key2}, "value2");
        is($clone->{key3}, "value3");
    }

    is($clone->{key1}, "value1");
    is($clone->{key2}, "value2");
    is($clone->{key3}, "value3");
}

{
    my $db = DBM::Deep->new(
        file => $filename,
    );

    is($db->{key1}, "value1");
    is($db->{key2}, "value2");
    is($db->{key3}, "value3");
}
