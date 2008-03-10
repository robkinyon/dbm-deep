##
# DBM::Deep Test
##
use strict;
use Test::More tests => 15;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
    fh => $fh,
    locking => 1,
    autoflush => 1,
    num_txns  => 16,
);

seek $db->_get_self->_storage->{fh}, 0, 0;

my $db2 = DBM::Deep->new(
    file => $filename,
    fh => $fh,
    locking => 1,
    autoflush => 1,
    num_txns  => 16,
);

$db->{foo} = 5;
$db->{bar} = $db->{foo};

is( $db->{foo}, 5, "Foo is still 5" );
is( $db->{bar}, 5, "Bar is now 5" );

$db->{foo} = 6;

is( $db->{foo}, 6, "Foo is now 6" );
is( $db->{bar}, 5, "Bar is still 5" );

$db->{foo} = [ 1 .. 3 ];
$db->{bar} = $db->{foo};

is( $db->{foo}[1], 2, "Foo[1] is still 2" );
is( $db->{bar}[1], 2, "Bar[1] is now 2" );

$db->{foo}[3] = 42;

is( $db->{foo}[3], 42, "Foo[3] is now 42" );
is( $db->{bar}[3], 42, "Bar[3] is also 42" );

delete $db->{foo};
is( $db->{bar}[3], 42, "After delete Foo, Bar[3] is still 42" );

$db->{foo} = $db->{bar};
$db2->begin_work;

    delete $db2->{bar};
    delete $db2->{foo};

    is( $db2->{bar}, undef, "It's deleted in the transaction" );
    is( $db->{bar}[3], 42, "... but not in the main" );

$db2->rollback;

# Why hasn't this failed!? Is it because stuff isn't getting deleted as expected?
# I need a test that walks the sectors
is( $db->{bar}[3], 42, "After delete Foo, Bar[3] is still 42" );
is( $db2->{bar}[3], 42, "After delete Foo, Bar[3] is still 42" );

delete $db->{foo};

is( $db->{bar}[3], 42, "After delete Foo, Bar[3] is still 42" );

__END__
warn "-2\n";
$db2->begin_work;

warn "-1\n";
  delete $db2->{bar};

warn "0\n";
$db2->commit;

warn "1\n";
ok( !exists $db->{bar}, "After commit, bar is gone" );
warn "2\n";
