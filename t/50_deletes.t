use strict;
use warnings FATAL => 'all';

use Test::More;

use t::common qw( new_dbm );

my $max = 10;

use_ok( 'DBM::Deep' );

my $dbm_factory = new_dbm();
while ( my $dbm_maker = $dbm_factory->() ) {
    my $db = $dbm_maker->();

    my $x = 1;
    while( $x <= $max ) {
        eval {
            delete $db->{borked}{test};
            $db->{borked}{test} = 1;
        };

        ok(!$@, "No eval failure after ${x}th iteration");
        $x++;
    }
}

done_testing;
