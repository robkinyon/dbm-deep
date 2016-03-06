use strict;
use warnings FATAL => 'all';

use Test::More;
use Test::Exception;
use t::common qw( new_dbm );
use Scalar::Util qw( reftype );

use_ok( 'DBM::Deep' );

# This is for https://rt.cpan.org/Ticket/Display.html?id=112059

my $dbm_factory = new_dbm();
while ( my $dbm_maker = $dbm_factory->() ) {
    my $db = $dbm_maker->();

    my $key = join chr(9), 'a', 'b';
    my $value = join chr(1), 'a', 'b', 'c', 'd', 'e';

    $db->{$key} = $value;
    is($db->{$key}, $value, "Can store and retrieve with chr(9)/chr(1)");
}

done_testing;
