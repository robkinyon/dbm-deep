use strict;
use Test::More tests => 13;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db1 = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
);

my $db2 = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
);

$db1->{x} = 'y';
is( $db1->{x}, 'y', "Before transaction, DB1's X is Y" );
is( $db2->{x}, 'y', "Before transaction, DB2's X is Y" );

$db1->begin_work;

is( $db1->{x}, 'y', "DB1 transaction started, no actions - DB1's X is Y" );
is( $db2->{x}, 'y', "DB1 transaction started, no actions - DB2's X is Y" );

$db1->{x} = 'z';
is( $db1->{x}, 'z', "Within DB1 transaction, DB1's X is Z" );
is( $db2->{x}, 'y', "Within DB1 transaction, DB2's X is still Y" );

$db2->{other_x} = 'foo';
is( $db2->{other_x}, 'foo', "DB2 set other_x within DB1's transaction, so DB2 can see it" );
is( $db1->{other_x}, undef, "Since other_x was added after the transaction began, DB1 doesn't see it." );

$db1->commit;

TODO: {
    local $TODO = 'Need to finish auditing first before commit will work.';
    is( $db1->{x}, 'z', "After commit, DB1's X is Y" );
    is( $db2->{x}, 'z', "After commit, DB2's X is Y" );
}

is( $db1->{other_x}, 'foo', "After DB1 transaction is over, DB1 can see other_x" );
is( $db2->{other_x}, 'foo', "After DB1 transaction is over, DB2 can still see other_x" );
