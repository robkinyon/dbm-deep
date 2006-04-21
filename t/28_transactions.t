use strict;
use Test::More tests => 31;
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

$db1->rollback;

is( $db1->{x}, 'y', "After rollback, DB1's X is Y" );
is( $db2->{x}, 'y', "After rollback, DB2's X is Y" );

is( $db1->{other_x}, 'foo', "After DB1 transaction is over, DB1 can see other_x" );
is( $db2->{other_x}, 'foo', "After DB1 transaction is over, DB2 can still see other_x" );

$db1->begin_work;

    is( $db1->{x}, 'y', "DB1 transaction started, no actions - DB1's X is Y" );
    is( $db2->{x}, 'y', "DB1 transaction started, no actions - DB2's X is Y" );

    $db1->{x} = 'z';
    is( $db1->{x}, 'z', "Within DB1 transaction, DB1's X is Z" );
    is( $db2->{x}, 'y', "Within DB1 transaction, DB2's X is still Y" );

$db1->commit;

is( $db1->{x}, 'z', "After commit, DB1's X is Z" );
is( $db2->{x}, 'z', "After commit, DB2's X is Z" );

$db1->begin_work;

    delete $db2->{other_x};
    is( $db2->{other_x}, undef, "DB2 deleted other_x in DB1's transaction, so it can't see it anymore" );
    is( $db1->{other_x}, 'foo', "Since other_x was deleted after the transaction began, DB1 still sees it." );

    delete $db1->{x};
    is( $db1->{x}, undef, "DB1 deleted X in a transaction, so it can't see it anymore" );
    is( $db2->{x}, 'z', "But, DB2 can still see it" );

$db1->rollback;

is( $db2->{other_x}, undef, "It's still deleted for DB2" );
is( $db1->{other_x}, undef, "And now DB1 sees the deletion" );

is( $db1->{x}, 'z', "The transaction was rolled back, so DB1 can see X now" );
is( $db2->{x}, 'z', "DB2 can still see it" );

$db1->begin_work;

    delete $db1->{x};
    is( $db1->{x}, undef, "DB1 deleted X in a transaction, so it can't see it anymore" );
    is( $db2->{x}, 'z', "But, DB2 can still see it" );

$db1->commit;

is( $db1->{x}, undef, "The transaction was committed, so DB1 still deleted X" );
is( $db2->{x}, undef, "DB2 can now see the deletion of X" );
