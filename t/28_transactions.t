use strict;
use Test::More tests => 4;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
);

$db->{x} = 'y';
is( $db->{x}, 'y' );
$db->begin_work;
$db->{x} = 'z';
is( $db->{x}, 'z' );
$db->rollback;
TODO: {
    local $TODO = "Haven't written transaction code yet";
    is( $db->{x}, 'y' );
}

# Add a commit test (using fork) - we don't have to use fork initially. Since
# the transaction is in the Engine object and each new() provides a new Engine
# object, we're cool.

# Should the transaction be in the Root and not the Engine? How would that
# work?

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
