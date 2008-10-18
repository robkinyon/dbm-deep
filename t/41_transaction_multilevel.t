use strict;
use Test::More tests => 41;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db1 = DBM::Deep->new(
    file => $filename,
    fh => $fh,
    locking => 1,
    autoflush => 1,
    num_txns  => 2,
);
seek $db1->_get_self->_engine->storage->{fh}, 0, 0;

my $db2 = DBM::Deep->new(
    file => $filename,
    fh => $fh,
    locking => 1,
    autoflush => 1,
    num_txns  => 2,
);

$db1->{x} = { xy => { foo => 'y' } };
is( $db1->{x}{xy}{foo}, 'y', "Before transaction, DB1's X is Y" );
is( $db2->{x}{xy}{foo}, 'y', "Before transaction, DB2's X is Y" );

$db1->begin_work;

    cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( xy )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( xy )], "DB2->X keys correct" );

    cmp_bag( [ keys %{$db1->{x}{xy}} ], [qw( foo )], "DB1->X->XY keys correct" );
    cmp_bag( [ keys %{$db2->{x}{xy}} ], [qw( foo )], "DB2->X->XY keys correct" );

    is( $db1->{x}{xy}{foo}, 'y', "After transaction, DB1's X is Y" );
    is( $db2->{x}{xy}{foo}, 'y', "After transaction, DB2's X is Y" );

    $db1->{x} = { yz => { bar => 30 } };
    ok( !exists $db1->{x}{xy}, "DB1: After reassignment of X, X->XY is gone" );
    is( $db2->{x}{xy}{foo}, 'y', "DB2: After reassignment of DB1 X, X is Y" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( yz )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( xy )], "DB2->X keys correct" );

$db1->rollback;

cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

cmp_bag( [ keys %{$db1->{x}} ], [qw( xy )], "DB1->X keys correct" );
cmp_bag( [ keys %{$db2->{x}} ], [qw( xy )], "DB2->X keys correct" );

cmp_bag( [ keys %{$db1->{x}{xy}} ], [qw( foo )], "DB1->X->XY keys correct" );
cmp_bag( [ keys %{$db2->{x}{xy}} ], [qw( foo )], "DB2->X->XY keys correct" );

is( $db1->{x}{xy}{foo}, 'y', "Before transaction, DB1's X is Y" );
is( $db2->{x}{xy}{foo}, 'y', "Before transaction, DB2's X is Y" );

$db1->begin_work;

    cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
    cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( xy )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( xy )], "DB2->X keys correct" );

    cmp_bag( [ keys %{$db1->{x}{xy}} ], [qw( foo )], "DB1->X->XY keys correct" );
    cmp_bag( [ keys %{$db2->{x}{xy}} ], [qw( foo )], "DB2->X->XY keys correct" );

    is( $db1->{x}{xy}{foo}, 'y', "After transaction, DB1's X is Y" );
    is( $db2->{x}{xy}{foo}, 'y', "After transaction, DB2's X is Y" );

    $db1->{x} = { yz => { bar => 30 } };
    ok( !exists $db1->{x}{xy}, "DB1: After reassignment of X, X->XY is gone" );
    is( $db2->{x}{xy}{foo}, 'y', "DB2: After reassignment of DB1 X, X->YZ is Y" );

    cmp_bag( [ keys %{$db1->{x}} ], [qw( yz )], "DB1->X keys correct" );
    cmp_bag( [ keys %{$db2->{x}} ], [qw( xy )], "DB2->X keys correct" );

$db1->commit;

cmp_bag( [ keys %$db1 ], [qw( x )], "DB1 keys correct" );
cmp_bag( [ keys %$db2 ], [qw( x )], "DB2 keys correct" );

cmp_bag( [ keys %{$db1->{x}} ], [qw( yz )], "DB1->X keys correct" );
cmp_bag( [ keys %{$db2->{x}} ], [qw( yz )], "DB2->X keys correct" );

cmp_bag( [ keys %{$db1->{x}{yz}} ], [qw( bar )], "DB1->X->XY keys correct" );
cmp_bag( [ keys %{$db2->{x}{yz}} ], [qw( bar )], "DB2->X->XY keys correct" );

$db1->_get_self->_engine->storage->close( $db1->_get_self );
$db2->_get_self->_engine->storage->close( $db2->_get_self );
