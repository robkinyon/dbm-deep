##
# DBM::Deep Test
##
use strict;
use Test::More tests => 7;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( $filename );

$db->{key1} = "value1";
is( $db->{key1}, "value1", "Value set correctly" );

# Testing to verify that the close() will occur if open is called on an open DB.
#XXX WOW is this hacky ...
$db->_get_self->{engine}->open( $db->_get_self );
is( $db->{key1}, "value1", "Value still set after re-open" );

throws_ok {
    my $db = DBM::Deep->new( 't' );
} qr/^DBM::Deep: Cannot sysopen file 't': /, "Can't open a file we aren't allowed to touch";

throws_ok {
    my $db = DBM::Deep->new( __FILE__ );
} qr/^DBM::Deep: Signature not found -- file is not a Deep DB/, "Only DBM::Deep DB files will be opened";

{
    my $db = DBM::Deep->new(
        file => $filename,
        locking => 1,
    );
    $db->_get_self->{engine}->close_fh( $db->_get_self );
    ok( !$db->lock );
}

{
    my $db = DBM::Deep->new(
        file => $filename,
        locking => 1,
    );
    $db->lock;
    $db->_get_self->{engine}->close_fh( $db->_get_self );
    ok( !$db->unlock );
}
