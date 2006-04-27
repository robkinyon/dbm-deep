use strict;
use Test::More tests => 62;
use Test::Deep;
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

    cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x other_x )], "DB2 keys correct" );

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

    $db2->{other_x} = 'bar';
    is( $db2->{other_x}, 'bar', "DB2 set other_x within DB1's transaction, so DB2 can see it" );
    is( $db1->{other_x}, 'foo', "Since other_x was modified after the transaction began, DB1 doesn't see the change." );

    cmp_bag( [ keys %$db1 ], [qw( x other_x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x other_x )], "DB2 keys correct" );

$db1->commit;

is( $db1->{x}, 'z', "After commit, DB1's X is Z" );
is( $db2->{x}, 'z', "After commit, DB2's X is Z" );

$db1->begin_work;

    delete $db2->{other_x};
    ok( !exists $db2->{other_x}, "DB2 deleted other_x in DB1's transaction, so it can't see it anymore" );
    is( $db1->{other_x}, 'bar', "Since other_x was deleted after the transaction began, DB1 still sees it." );

    cmp_bag( [ keys %$db1 ], [qw( x other_x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

    delete $db1->{x};
    ok( !exists $db1->{x}, "DB1 deleted X in a transaction, so it can't see it anymore" );
    is( $db2->{x}, 'z', "But, DB2 can still see it" );

    cmp_bag( [ keys %$db1 ], [qw( other_x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

$db1->rollback;

ok( !exists $db2->{other_x}, "It's still deleted for DB2" );
ok( !exists $db1->{other_x}, "And now DB1 sees the deletion" );

is( $db1->{x}, 'z', "The transaction was rolled back, so DB1 can see X now" );
is( $db2->{x}, 'z', "DB2 can still see it" );

cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

$db1->begin_work;

    delete $db1->{x};
    ok( !exists $db1->{x}, "DB1 deleted X in a transaction, so it can't see it anymore" );
#__END__
    is( $db2->{x}, 'z', "But, DB2 can still see it" );

    cmp_bag( [ keys %$db1 ], [qw()], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

$db1->commit;

ok( !exists $db1->{x}, "The transaction was committed, so DB1 still deleted X" );
ok( !exists $db2->{x}, "DB2 can now see the deletion of X" );

$db1->{foo} = 'bar';
is( $db1->{foo}, 'bar', "Set foo to bar in DB1" );
is( $db2->{foo}, 'bar', "Set foo to bar in DB2" );

cmp_bag( [ keys %$db1 ], [qw( foo )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( foo )], "DB2 keys correct" );

$db1->begin_work;

    %$db1 = (); # clear()
    ok( !exists $db1->{foo}, "Cleared foo" );
    is( $db2->{foo}, 'bar', "But in DB2, we can still see it" );

    cmp_bag( [ keys %$db1 ], [qw()], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( foo )], "DB2 keys correct" );

$db1->rollback;

is( $db1->{foo}, 'bar', "Rollback means 'foo' is still there" );
is( $db2->{foo}, 'bar', "Rollback means 'foo' is still there" );

cmp_bag( [ keys %$db1 ], [qw( foo )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( foo )], "DB2 keys correct" );

$db1->optimize;

is( $db1->{foo}, 'bar', 'After optimize, everything is ok' );
is( $db2->{foo}, 'bar', 'After optimize, everything is ok' );

cmp_bag( [ keys %$db1 ], [qw( foo )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( foo )], "DB2 keys correct" );

$db1->begin_work;

    cmp_ok( $db1->_fileobj->transaction_id, '==', 1, "Transaction ID has been reset after optimize" );

$db1->rollback;

__END__

Tests to add:
* Two transactions running at the same time
* Doing a clear on the head while a transaction is running
# More than just two keys
