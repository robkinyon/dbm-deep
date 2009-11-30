package DBM::Deep::Engine;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Iterator ();

# File-wide notes:
# * Every method in here assumes that the storage has been appropriately
#   safeguarded. This can be anything from flock() to some sort of manual
#   mutex. But, it's the caller's responsability to make sure that this has
#   been done.

# Setup file and tag signatures.  These should never change.
sub SIG_FILE     () { 'DPDB' }
sub SIG_HEADER   () { 'h'    }
sub SIG_HASH     () { 'H'    }
sub SIG_ARRAY    () { 'A'    }
sub SIG_NULL     () { 'N'    }
sub SIG_DATA     () { 'D'    }
sub SIG_INDEX    () { 'I'    }
sub SIG_BLIST    () { 'B'    }
sub SIG_FREE     () { 'F'    }
sub SIG_SIZE     () {  1     }

=head1 NAME

DBM::Deep::Engine

=head1 PURPOSE

This is an internal-use-only object for L<DBM::Deep/>. It mediates the low-level
mapping between the L<DBM::Deep/> objects and the storage medium.

The purpose of this documentation is to provide low-level documentation for
developers. It is B<not> intended to be used by the general public. This
documentation and what it documents can and will change without notice.

=head1 OVERVIEW

The engine exposes an API to the DBM::Deep objects (DBM::Deep, DBM::Deep::Array,
and DBM::Deep::Hash) for their use to access the actual stored values. This API
is the following:

=over 4

=item * new

=item * read_value

=item * get_classname

=item * make_reference

=item * key_exists

=item * delete_key

=item * write_value

=item * get_next_key

=item * setup_fh

=item * begin_work

=item * commit

=item * rollback

=item * lock_exclusive

=item * lock_shared

=item * unlock

=back

They are explained in their own sections below. These methods, in turn, may
provide some bounds-checking, but primarily act to instantiate objects in the
Engine::Sector::* hierarchy and dispatch to them.

=head1 TRANSACTIONS

Transactions in DBM::Deep are implemented using a variant of MVCC. This attempts
to keep the amount of actual work done against the file low while stil providing
Atomicity, Consistency, and Isolation. Durability, unfortunately, cannot be done
with only one file.

=head2 STALENESS

If another process uses a transaction slot and writes stuff to it, then
terminates, the data that process wrote it still within the file. In order to
address this, there is also a transaction staleness counter associated within
every write.  Each time a transaction is started, that process increments that
transaction's staleness counter. If, when it reads a value, the staleness
counters aren't identical, DBM::Deep will consider the value on disk to be stale
and discard it.

=head2 DURABILITY

The fourth leg of ACID is Durability, the guarantee that when a commit returns,
the data will be there the next time you read from it. This should be regardless
of any crashes or powerdowns in between the commit and subsequent read.
DBM::Deep does provide that guarantee; once the commit returns, all of the data
has been transferred from the transaction shadow to the HEAD. The issue arises
with partial commits - a commit that is interrupted in some fashion. In keeping
with DBM::Deep's "tradition" of very light error-checking and non-existent
error-handling, there is no way to recover from a partial commit. (This is
probably a failure in Consistency as well as Durability.)

Other DBMSes use transaction logs (a separate file, generally) to achieve
Durability.  As DBM::Deep is a single-file, we would have to do something
similar to what SQLite and BDB do in terms of committing using synchonized
writes. To do this, we would have to use a much higher RAM footprint and some
serious programming that make my head hurts just to think about it.

=cut



=head2 get_next_key( $obj, $prev_key )

This takes an object that provides _base_offset() and an optional string
representing the prior key returned via a prior invocation of this method.

This method delegates to C<< DBM::Deep::Iterator->get_next_key() >>.

=cut

# XXX Add staleness here
sub get_next_key {
    my $self = shift;
    my ($obj, $prev_key) = @_;

    # XXX Need to add logic about resetting the iterator if any key in the reference has changed
    unless ( $prev_key ) {
        $obj->{iterator} = DBM::Deep::Iterator->new({
            base_offset => $obj->_base_offset,
            engine      => $self,
        });
    }

    return $obj->{iterator}->get_next_key( $obj );
}

1;
__END__
