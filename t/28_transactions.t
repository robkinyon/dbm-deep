use strict;
use Test::More tests => 13;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db1 = DBM::Deep->new(
    file => $filename,
    locking => 1,
);

my $db2 = DBM::Deep->new(
    file => $filename,
    locking => 1,
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
is( $db2->{other_x}, 'foo', "Set other_x within DB1's transaction, so DB2 can see it" );
is( $db1->{other_x}, undef, "Since other_x was added after the transaction began, DB1 doesn't see it." );

$db1->rollback;

is( $db1->{x}, 'y', "After rollback, DB1's X is Y" );
is( $db2->{x}, 'y', "After rollback, DB2's X is Y" );

is( $db1->{other_x}, 'foo', "After DB1 transaction is over, DB1 can see other_x" );
is( $db2->{other_x}, 'foo', "After DB1 transaction is over, DB2 can still see other_x" );

# Add a commit test (using fork) - we don't have to use fork initially. Since
# the transaction is in the Engine object and each new() provides a new Engine
# object, we're cool.

# Should the transaction be in the Root and not the Engine? How would that
# work?

# What about the following:
#   $db->{foo} = {};
#   $db2 = $db->{foo};
#   $db2->begin_work;
#   $db->{foo} = 3;

__END__

Plan for transactions:
* In a normal world, every key's version is set to 0. 0 is the indication that
  this value isn't part of a transaction.
* When a transaction is started, it is assigned the next transaction number.
  The engine handles the transaction, not the DBM::Deep object.
* While the transaction is running, all mutations occur in parallel, not
  overwriting the original. They are assigned the transaction number.
* How is a parallel mutation handled? It needs to be handled in the file
  because we don't who's going to access what from where?
    - Well, everything has to go through the same Engine object.
    - Two processes may never access the same transaction.
    - If a process in the middle of a transaction dies, the transaction is
      considered void and will be reaped during the next optimize().
    - So, in theory, by storing the fact that -this- file offset is involved
      in a transaction should be able to be stored in memory.
    - 

* Every operation is now transaction-aware
* If a transaction is in effect against the file, everyone ELSE has to be
  aware of it and respect it
* Every key now has a transaction number associated with it
* Every operation only operates against the key with the appropriate
  transaction number
* In the case of %$db = (), there will need to be a 0th level to tell you
  which $db to go to.
* Transaction #0 is the HEAD.
* Upon commit, your version of reality is overlaid upon the HEAD.
* Upon rollback, your version of reality disappears.
* Upon process termination, an attempt is made to rollback any pending
  transaction(s). If ABEND, it's your responsability to optimize().
* The exact actions for each tie-method will have to be mapped out.
