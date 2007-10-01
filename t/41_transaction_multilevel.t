use strict;
use Test::More tests => 33;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db1 = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
    num_txns  => 2,
);

my $db2 = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
    num_txns  => 2,
);

$db1->{x} = { foo => 'y' };
is( $db1->{x}{foo}, 'y', "Before transaction, DB1's X is Y" );
is( $db2->{x}{foo}, 'y', "Before transaction, DB2's X is Y" );

$db1->begin_work;

    cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( foo )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( foo )], "DB2->X keys correct" );

    is( $db1->{x}{foo}, 'y', "After transaction, DB1's X is Y" );
    is( $db2->{x}{foo}, 'y', "After transaction, DB2's X is Y" );

    $db1->{x} = { bar => 30 };
    ok( !exists $db1->{x}{foo}, "DB1: After reassignment of X, X->FOO is gone" );
    is( $db2->{x}{foo}, 'y', "DB2: After reassignment of DB1 X, X is Y" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( bar )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( foo )], "DB2->X keys correct" );

$db1->rollback;

cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

cmp_bag( [ keys %{$db1->{x}} ], [qw( foo )], "DB1->X keys correct" );
cmp_bag( [ keys %{$db2->{x}} ], [qw( foo )], "DB2->X keys correct" );

is( $db1->{x}{foo}, 'y', "Before transaction, DB1's X is Y" );
is( $db2->{x}{foo}, 'y', "Before transaction, DB2's X is Y" );

$db1->begin_work;

    cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( foo )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( foo )], "DB2->X keys correct" );

    is( $db1->{x}{foo}, 'y', "After transaction, DB1's X is Y" );
    is( $db2->{x}{foo}, 'y', "After transaction, DB2's X is Y" );

    $db1->{x} = { bar => 30 };
    ok( !exists $db1->{x}{foo}, "DB1: After reassignment of X, X->FOO is gone" );
    is( $db2->{x}{foo}, 'y', "DB2: After reassignment of DB1 X, X is Y" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( bar )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( foo )], "DB2->X keys correct" );

$db1->commit;

cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

cmp_bag( [ keys %{$db1->{x}} ], [qw( bar )], "DB1->X keys correct" );
cmp_bag( [ keys %{$db2->{x}} ], [qw( bar )], "DB2->X keys correct" );
