package DBM::Deep::Engine;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

# Never import symbols into our namespace. We are a class, not a library.
# -RobK, 2008-05-27
use Scalar::Util ();

#use Data::Dumper ();

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

use DBM::Deep::Iterator::BucketList ();
use DBM::Deep::Iterator::Index ();
use DBM::Deep::Engine::Sector::Data ();
use DBM::Deep::Engine::Sector::BucketList ();
use DBM::Deep::Engine::Sector::Index ();
use DBM::Deep::Engine::Sector::Null ();
use DBM::Deep::Engine::Sector::Reference ();
use DBM::Deep::Engine::Sector::Scalar ();
use DBM::Deep::Null ();

my $STALE_SIZE = 2;

# Please refer to the pack() documentation for further information
my %StP = (
    1 => 'C', # Unsigned char value (no order needed as it's just one byte)
    2 => 'n', # Unsigned short in "network" (big-endian) order
    4 => 'N', # Unsigned long in "network" (big-endian) order
    8 => 'Q', # Usigned quad (no order specified, presumably machine-dependent)
);

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

If another process uses a transaction slot and writes stuff to it, then terminates,
the data that process wrote it still within the file. In order to address this,
there is also a transaction staleness counter associated within every write.
Each time a transaction is started, that process increments that transaction's
staleness counter. If, when it reads a value, the staleness counters aren't
identical, DBM::Deep will consider the value on disk to be stale and discard it.

=head2 DURABILITY

The fourth leg of ACID is Durability, the guarantee that when a commit returns,
the data will be there the next time you read from it. This should be regardless
of any crashes or powerdowns in between the commit and subsequent read. DBM::Deep
does provide that guarantee; once the commit returns, all of the data has been
transferred from the transaction shadow to the HEAD. The issue arises with partial
commits - a commit that is interrupted in some fashion. In keeping with DBM::Deep's
"tradition" of very light error-checking and non-existent error-handling, there is
no way to recover from a partial commit. (This is probably a failure in Consistency
as well as Durability.)

Other DBMSes use transaction logs (a separate file, generally) to achieve Durability.
As DBM::Deep is a single-file, we would have to do something similar to what SQLite
and BDB do in terms of committing using synchonized writes. To do this, we would have
to use a much higher RAM footprint and some serious programming that make my head
hurts just to think about it.

=head1 EXTERNAL METHODS

=head2 new()

This takes a set of args. These args are described in the documentation for
L<DBM::Deep/new>.

=cut

sub new {
    my $class = shift;
    my ($args) = @_;

    $args->{storage} = DBM::Deep::File->new( $args )
        unless exists $args->{storage};

    my $self = bless {
        byte_size   => 4,

        digest      => undef,
        hash_size   => 16,  # In bytes
        hash_chars  => 256, # Number of chars the algorithm uses per byte
        max_buckets => 16,
        num_txns    => 1,   # The HEAD
        trans_id    => 0,   # Default to the HEAD

        data_sector_size => 64, # Size in bytes of each data sector

        entries => {}, # This is the list of entries for transactions
        storage => undef,
    }, $class;

    # Never allow byte_size to be set directly.
    delete $args->{byte_size};
    if ( defined $args->{pack_size} ) {
        if ( lc $args->{pack_size} eq 'small' ) {
            $args->{byte_size} = 2;
        }
        elsif ( lc $args->{pack_size} eq 'medium' ) {
            $args->{byte_size} = 4;
        }
        elsif ( lc $args->{pack_size} eq 'large' ) {
            $args->{byte_size} = 8;
        }
        else {
            DBM::Deep->_throw_error( "Unknown pack_size value: '$args->{pack_size}'" );
        }
    }

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }

    my %validations = (
        max_buckets      => { floor => 16, ceil => 256 },
        num_txns         => { floor => 1,  ceil => 255 },
        data_sector_size => { floor => 32, ceil => 256 },
    );

    while ( my ($attr, $c) = each %validations ) {
        if (   !defined $self->{$attr}
            || !length $self->{$attr}
            || $self->{$attr} =~ /\D/
            || $self->{$attr} < $c->{floor}
        ) {
            $self->{$attr} = '(undef)' if !defined $self->{$attr};
            warn "Floor of $attr is $c->{floor}. Setting it to $c->{floor} from '$self->{$attr}'\n";
            $self->{$attr} = $c->{floor};
        }
        elsif ( $self->{$attr} > $c->{ceil} ) {
            warn "Ceiling of $attr is $c->{ceil}. Setting it to $c->{ceil} from '$self->{$attr}'\n";
            $self->{$attr} = $c->{ceil};
        }
    }

    if ( !$self->{digest} ) {
        require Digest::MD5;
        $self->{digest} = \&Digest::MD5::md5;
    }

    return $self;
}

=head2 read_value( $obj, $key )

This takes an object that provides _base_offset() and a string. It returns the
value stored in the corresponding Sector::Value's data section.

=cut

sub read_value {
    my $self = shift;
    my ($obj, $key) = @_;

    # This will be a Reference sector
    my $sector = $self->_load_sector( $obj->_base_offset )
        or return;

    if ( $sector->staleness != $obj->_staleness ) {
        return;
    }

    my $key_md5 = $self->_apply_digest( $key );

    my $value_sector = $sector->get_data_for({
        key_md5    => $key_md5,
        allow_head => 1,
    });

    unless ( $value_sector ) {
        $value_sector = DBM::Deep::Engine::Sector::Null->new({
            engine => $self,
            data   => undef,
        });

        $sector->write_data({
            key_md5 => $key_md5,
            key     => $key,
            value   => $value_sector,
        });
    }

    return $value_sector->data;
}

=head2 get_classname( $obj )

This takes an object that provides _base_offset() and returns the classname (if any)
associated with it.

It delegates to Sector::Reference::get_classname() for the heavy lifting.

It performs a staleness check.

=cut

sub get_classname {
    my $self = shift;
    my ($obj) = @_;

    # This will be a Reference sector
    my $sector = $self->_load_sector( $obj->_base_offset )
        or DBM::Deep->_throw_error( "How did get_classname fail (no sector for '$obj')?!" );

    if ( $sector->staleness != $obj->_staleness ) {
        return;
    }

    return $sector->get_classname;
}

=head2 make_reference( $obj, $old_key, $new_key )

This takes an object that provides _base_offset() and two strings. The
strings correspond to the old key and new key, respectively. This operation
is equivalent to (given C<< $db->{foo} = []; >>) C<< $db->{bar} = $db->{foo}; >>.

This returns nothing.

=cut

sub make_reference {
    my $self = shift;
    my ($obj, $old_key, $new_key) = @_;

    # This will be a Reference sector
    my $sector = $self->_load_sector( $obj->_base_offset )
        or DBM::Deep->_throw_error( "How did make_reference fail (no sector for '$obj')?!" );

    if ( $sector->staleness != $obj->_staleness ) {
        return;
    }

    my $old_md5 = $self->_apply_digest( $old_key );

    my $value_sector = $sector->get_data_for({
        key_md5    => $old_md5,
        allow_head => 1,
    });

    unless ( $value_sector ) {
        $value_sector = DBM::Deep::Engine::Sector::Null->new({
            engine => $self,
            data   => undef,
        });

        $sector->write_data({
            key_md5 => $old_md5,
            key     => $old_key,
            value   => $value_sector,
        });
    }

    if ( $value_sector->isa( 'DBM::Deep::Engine::Sector::Reference' ) ) {
        $sector->write_data({
            key     => $new_key,
            key_md5 => $self->_apply_digest( $new_key ),
            value   => $value_sector,
        });
        $value_sector->increment_refcount;
    }
    else {
        $sector->write_data({
            key     => $new_key,
            key_md5 => $self->_apply_digest( $new_key ),
            value   => $value_sector->clone,
        });
    }

    return;
}

=head2 key_exists( $obj, $key )

This takes an object that provides _base_offset() and a string for
the key to be checked. This returns 1 for true and "" for false.

=cut

sub key_exists {
    my $self = shift;
    my ($obj, $key) = @_;

    # This will be a Reference sector
    my $sector = $self->_load_sector( $obj->_base_offset )
        or return '';

    if ( $sector->staleness != $obj->_staleness ) {
        return '';
    }

    my $data = $sector->get_data_for({
        key_md5    => $self->_apply_digest( $key ),
        allow_head => 1,
    });

    # exists() returns 1 or '' for true/false.
    return $data ? 1 : '';
}

=head2 delete_key( $obj, $key )

This takes an object that provides _base_offset() and a string for
the key to be deleted. This returns the result of the Sector::Reference
delete_key() method.

=cut

sub delete_key {
    my $self = shift;
    my ($obj, $key) = @_;

    my $sector = $self->_load_sector( $obj->_base_offset )
        or return;

    if ( $sector->staleness != $obj->_staleness ) {
        return;
    }

    return $sector->delete_key({
        key_md5    => $self->_apply_digest( $key ),
        allow_head => 0,
    });
}

=head2 write_value( $obj, $key, $value )

This takes an object that provides _base_offset(), a string for the
key, and a value. This value can be anything storable within L<DBM::Deep/>.

This returns 1 upon success.

=cut

sub write_value {
    my $self = shift;
    my ($obj, $key, $value) = @_;

    my $r = Scalar::Util::reftype( $value ) || '';
    {
        last if $r eq '';
        last if $r eq 'HASH';
        last if $r eq 'ARRAY';

        DBM::Deep->_throw_error(
            "Storage of references of type '$r' is not supported."
        );
    }

    # This will be a Reference sector
    my $sector = $self->_load_sector( $obj->_base_offset )
        or DBM::Deep->_throw_error( "Cannot write to a deleted spot in DBM::Deep." );

    if ( $sector->staleness != $obj->_staleness ) {
        DBM::Deep->_throw_error( "Cannot write to a deleted spot in DBM::Deep." );
    }

    my ($class, $type);
    if ( !defined $value ) {
        $class = 'DBM::Deep::Engine::Sector::Null';
    }
    elsif ( $r eq 'ARRAY' || $r eq 'HASH' ) {
        my $tmpvar;
        if ( $r eq 'ARRAY' ) {
            $tmpvar = tied @$value;
        } elsif ( $r eq 'HASH' ) {
            $tmpvar = tied %$value;
        }

        if ( $tmpvar ) {
            my $is_dbm_deep = eval { local $SIG{'__DIE__'}; $tmpvar->isa( 'DBM::Deep' ); };

            unless ( $is_dbm_deep ) {
                DBM::Deep->_throw_error( "Cannot store something that is tied." );
            }

            unless ( $tmpvar->_engine->storage == $self->storage ) {
                DBM::Deep->_throw_error( "Cannot store values across DBM::Deep files. Please use export() instead." );
            }

            # First, verify if we're storing the same thing to this spot. If we are, then
            # this should be a no-op. -EJS, 2008-05-19
            my $loc = $sector->get_data_location_for({
                key_md5 => $self->_apply_digest( $key ),
                allow_head => 1,
            });

            if ( defined($loc) && $loc == $tmpvar->_base_offset ) {
                return 1;
            }

            #XXX Can this use $loc?
            my $value_sector = $self->_load_sector( $tmpvar->_base_offset );
            $sector->write_data({
                key     => $key,
                key_md5 => $self->_apply_digest( $key ),
                value   => $value_sector,
            });
            $value_sector->increment_refcount;

            return 1;
        }

        $class = 'DBM::Deep::Engine::Sector::Reference';
        $type = substr( $r, 0, 1 );
    }
    else {
        if ( tied($value) ) {
            DBM::Deep->_throw_error( "Cannot store something that is tied." );
        }
        $class = 'DBM::Deep::Engine::Sector::Scalar';
    }

    # Create this after loading the reference sector in case something bad happens.
    # This way, we won't allocate value sector(s) needlessly.
    my $value_sector = $class->new({
        engine => $self,
        data   => $value,
        type   => $type,
    });

    $sector->write_data({
        key     => $key,
        key_md5 => $self->_apply_digest( $key ),
        value   => $value_sector,
    });

    # This code is to make sure we write all the values in the $value to the disk
    # and to make sure all changes to $value after the assignment are reflected
    # on disk. This may be counter-intuitive at first, but it is correct dwimmery.
    #   NOTE - simply tying $value won't perform a STORE on each value. Hence, the
    # copy to a temp value.
    if ( $r eq 'ARRAY' ) {
        my @temp = @$value;
        tie @$value, 'DBM::Deep', {
            base_offset => $value_sector->offset,
            staleness   => $value_sector->staleness,
            storage     => $self->storage,
            engine      => $self,
        };
        @$value = @temp;
        bless $value, 'DBM::Deep::Array' unless Scalar::Util::blessed( $value );
    }
    elsif ( $r eq 'HASH' ) {
        my %temp = %$value;
        tie %$value, 'DBM::Deep', {
            base_offset => $value_sector->offset,
            staleness   => $value_sector->staleness,
            storage     => $self->storage,
            engine      => $self,
        };

        %$value = %temp;
        bless $value, 'DBM::Deep::Hash' unless Scalar::Util::blessed( $value );
    }

    return 1;
}

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

=head2 setup_fh( $obj )

This takes an object that provides _base_offset(). It will do everything needed
in order to properly initialize all values for necessary functioning. If this is
called upon an already initialized object, this will also reset the inode.

This returns 1.

=cut

sub setup_fh {
    my $self = shift;
    my ($obj) = @_;

    # We're opening the file.
    unless ( $obj->_base_offset ) {
        my $bytes_read = $self->_read_file_header;

        # Creating a new file
        unless ( $bytes_read ) {
            $self->_write_file_header;

            # 1) Create Array/Hash entry
            my $initial_reference = DBM::Deep::Engine::Sector::Reference->new({
                engine => $self,
                type   => $obj->_type,
            });
            $obj->{base_offset} = $initial_reference->offset;
            $obj->{staleness} = $initial_reference->staleness;

            $self->storage->flush;
        }
        # Reading from an existing file
        else {
            $obj->{base_offset} = $bytes_read;
            my $initial_reference = DBM::Deep::Engine::Sector::Reference->new({
                engine => $self,
                offset => $obj->_base_offset,
            });
            unless ( $initial_reference ) {
                DBM::Deep->_throw_error("Corrupted file, no master index record");
            }

            unless ($obj->_type eq $initial_reference->type) {
                DBM::Deep->_throw_error("File type mismatch");
            }

            $obj->{staleness} = $initial_reference->staleness;
        }
    }

    $self->storage->set_inode;

    return 1;
}

=head2 begin_work( $obj )

This takes an object that provides _base_offset(). It will set up all necessary
bookkeeping in order to run all work within a transaction.

If $obj is already within a transaction, an error wiill be thrown. If there are
no more available transactions, an error will be thrown.

This returns undef.

=cut

sub begin_work {
    my $self = shift;
    my ($obj) = @_;

    if ( $self->trans_id ) {
        DBM::Deep->_throw_error( "Cannot begin_work within an active transaction" );
    }

    my @slots = $self->read_txn_slots;
    my $found;
    for my $i ( 0 .. $#slots ) {
        next if $slots[$i];

        $slots[$i] = 1;
        $self->set_trans_id( $i + 1 );
        $found = 1;
        last;
    }
    unless ( $found ) {
        DBM::Deep->_throw_error( "Cannot allocate transaction ID" );
    }
    $self->write_txn_slots( @slots );

    if ( !$self->trans_id ) {
        DBM::Deep->_throw_error( "Cannot begin_work - no available transactions" );
    }

    return;
}

=head2 rollback( $obj )

This takes an object that provides _base_offset(). It will revert all
actions taken within the running transaction.

If $obj is not within a transaction, an error will be thrown.

This returns 1.

=cut

sub rollback {
    my $self = shift;
    my ($obj) = @_;

    if ( !$self->trans_id ) {
        DBM::Deep->_throw_error( "Cannot rollback without an active transaction" );
    }

    # Each entry is the file location for a bucket that has a modification for
    # this transaction. The entries need to be expunged.
    foreach my $entry (@{ $self->get_entries } ) {
        # Remove the entry here
        my $read_loc = $entry
          + $self->hash_size
          + $self->byte_size
          + $self->byte_size
          + ($self->trans_id - 1) * ( $self->byte_size + $STALE_SIZE );

        my $data_loc = $self->storage->read_at( $read_loc, $self->byte_size );
        $data_loc = unpack( $StP{$self->byte_size}, $data_loc );
        $self->storage->print_at( $read_loc, pack( $StP{$self->byte_size}, 0 ) );

        if ( $data_loc > 1 ) {
            $self->_load_sector( $data_loc )->free;
        }
    }

    $self->clear_entries;

    my @slots = $self->read_txn_slots;
    $slots[$self->trans_id-1] = 0;
    $self->write_txn_slots( @slots );
    $self->inc_txn_staleness_counter( $self->trans_id );
    $self->set_trans_id( 0 );

    return 1;
}

=head2 commit( $obj )

This takes an object that provides _base_offset(). It will apply all
actions taken within the transaction to the HEAD.

If $obj is not within a transaction, an error will be thrown.

This returns 1.

=cut

sub commit {
    my $self = shift;
    my ($obj) = @_;

    if ( !$self->trans_id ) {
        DBM::Deep->_throw_error( "Cannot commit without an active transaction" );
    }

    foreach my $entry (@{ $self->get_entries } ) {
        # Overwrite the entry in head with the entry in trans_id
        my $base = $entry
          + $self->hash_size
          + $self->byte_size;

        my $head_loc = $self->storage->read_at( $base, $self->byte_size );
        $head_loc = unpack( $StP{$self->byte_size}, $head_loc );

        my $spot = $base + $self->byte_size + ($self->trans_id - 1) * ( $self->byte_size + $STALE_SIZE );
        my $trans_loc = $self->storage->read_at(
            $spot, $self->byte_size,
        );

        $self->storage->print_at( $base, $trans_loc );
        $self->storage->print_at(
            $spot,
            pack( $StP{$self->byte_size} . ' ' . $StP{$STALE_SIZE}, (0) x 2 ),
        );

        if ( $head_loc > 1 ) {
            $self->_load_sector( $head_loc )->free;
        }
    }

    $self->clear_entries;

    my @slots = $self->read_txn_slots;
    $slots[$self->trans_id-1] = 0;
    $self->write_txn_slots( @slots );
    $self->inc_txn_staleness_counter( $self->trans_id );
    $self->set_trans_id( 0 );

    return 1;
}

=head2 lock_exclusive()

This takes an object that provides _base_offset(). It will guarantee that
the storage has taken precautions to be safe for a write.

This returns nothing.

=cut

sub lock_exclusive {
    my $self = shift;
    my ($obj) = @_;
    return $self->storage->lock_exclusive( $obj );
}

=head2 lock_shared()

This takes an object that provides _base_offset(). It will guarantee that
the storage has taken precautions to be safe for a read.

This returns nothing.

=cut

sub lock_shared {
    my $self = shift;
    my ($obj) = @_;
    return $self->storage->lock_shared( $obj );
}

=head2 unlock()

This takes an object that provides _base_offset(). It will guarantee that
the storage has released all locks taken.

This returns nothing.

=cut

sub unlock {
    my $self = shift;
    my ($obj) = @_;

    my $rv = $self->storage->unlock( $obj );

    $self->flush if $rv;

    return $rv;
}

=head1 INTERNAL METHODS

The following methods are internal-use-only to DBM::Deep::Engine.

=cut

=head2 read_txn_slots()

This takes no arguments.

This will return an array with a 1 or 0 in each slot. Each spot represents one
available transaction. If the slot is 1, that transaction is taken. If it is 0,
the transaction is available.

=cut

sub read_txn_slots {
    my $self = shift;
    my $bl = $self->txn_bitfield_len;
    my $num_bits = $bl * 8;
    return split '', unpack( 'b'.$num_bits,
        $self->storage->read_at(
            $self->trans_loc, $bl,
        )
    );
}

=head2 write_txn_slots( @slots )

This takes an array of 1's and 0's. This array represents the transaction slots
returned by L</read_txn_slots()>. In other words, the following is true:

  @x = read_txn_slots( write_txn_slots( @x ) );

(With the obviously missing object referents added back in.)

=cut

sub write_txn_slots {
    my $self = shift;
    my $num_bits = $self->txn_bitfield_len * 8;
    $self->storage->print_at( $self->trans_loc,
        pack( 'b'.$num_bits, join('', @_) ),
    );
}

=head2 get_running_txn_ids()

This takes no arguments.

This will return an array of taken transaction IDs. This wraps L</read_txn_slots()>.

=cut

sub get_running_txn_ids {
    my $self = shift;
    my @transactions = $self->read_txn_slots;
    my @trans_ids = map { $_+1 } grep { $transactions[$_] } 0 .. $#transactions;
}

=head2 get_txn_staleness_counter( $trans_id )

This will return the staleness counter for the given transaction ID. Please see
L</TRANSACTION STALENESS> for more information.

=cut

sub get_txn_staleness_counter {
    my $self = shift;
    my ($trans_id) = @_;

    # Hardcode staleness of 0 for the HEAD
    return 0 unless $trans_id;

    return unpack( $StP{$STALE_SIZE},
        $self->storage->read_at(
            $self->trans_loc + $self->txn_bitfield_len + $STALE_SIZE * ($trans_id - 1),
            $STALE_SIZE,
        )
    );
}

=head2 inc_txn_staleness_counter( $trans_id )

This will increment the staleness counter for the given transaction ID. Please see
L</TRANSACTION STALENESS> for more information.

=cut

sub inc_txn_staleness_counter {
    my $self = shift;
    my ($trans_id) = @_;

    # Hardcode staleness of 0 for the HEAD
    return 0 unless $trans_id;

    $self->storage->print_at(
        $self->trans_loc + $self->txn_bitfield_len + $STALE_SIZE * ($trans_id - 1),
        pack( $StP{$STALE_SIZE}, $self->get_txn_staleness_counter( $trans_id ) + 1 ),
    );
}

=head2 get_entries()

This takes no arguments.

This returns a list of all the sectors that have been modified by this transaction.

=cut

sub get_entries {
    my $self = shift;
    return [ keys %{ $self->{entries}{$self->trans_id} ||= {} } ];
}

=head2 add_entry( $trans_id, $location )

This takes a transaction ID and a file location and marks the sector at that location
as having been modified by the transaction identified by $trans_id.

This returns nothing.

B<NOTE>: Unlike all the other _entries() methods, there are several cases where
C<< $trans_id != $self->trans_id >> for this method.

=cut

sub add_entry {
    my $self = shift;
    my ($trans_id, $loc) = @_;

    $self->{entries}{$trans_id} ||= {};
    $self->{entries}{$trans_id}{$loc} = undef;
}

=head2 reindex_entry( $old_loc, $new_loc )

This takes two locations (old and new, respectively). If a location that has been
modified by this transaction is subsequently reindexed due to a bucketlist
overflowing, then the entries hash needs to be made aware of this change.

This returns nothing.

=cut

sub reindex_entry {
    my $self = shift;
    my ($old_loc, $new_loc) = @_;

    TRANS:
    while ( my ($trans_id, $locs) = each %{ $self->{entries} } ) {
        if ( exists $locs->{$old_loc} ) {
            delete $locs->{$old_loc};
            $locs->{$new_loc} = undef;
            next TRANS;
        }
    }
}

=head2 clear_entries()

This takes no arguments. It will clear the entries list for the running transaction.

This returns nothing.

=cut

sub clear_entries {
    my $self = shift;
    delete $self->{entries}{$self->trans_id};
}

=head2 _write_file_header()

This writes the file header for a new file. This will write the various settings
that set how the file is interpreted.

=head2 _read_file_header()

This reads the file header from an existing file. This will read the various
settings that set how the file is interpreted.

=cut

{
    my $header_fixed = length( SIG_FILE ) + 1 + 4 + 4;
    my $this_file_version = 3;

    sub _write_file_header {
        my $self = shift;

        my $nt = $self->num_txns;
        my $bl = $self->txn_bitfield_len;

        my $header_var = 1 + 1 + 1 + 1 + $bl + $STALE_SIZE * ($nt - 1) + 3 * $self->byte_size;

        my $loc = $self->storage->request_space( $header_fixed + $header_var );

        $self->storage->print_at( $loc,
            SIG_FILE,
            SIG_HEADER,
            pack('N', $this_file_version), # At this point, we're at 9 bytes
            pack('N', $header_var),        # header size
            # --- Above is $header_fixed. Below is $header_var
            pack('C', $self->byte_size),

            # These shenanigans are to allow a 256 within a C
            pack('C', $self->max_buckets - 1),
            pack('C', $self->data_sector_size - 1),

            pack('C', $nt),
            pack('C' . $bl, 0 ),                           # Transaction activeness bitfield
            pack($StP{$STALE_SIZE}.($nt-1), 0 x ($nt-1) ), # Transaction staleness counters
            pack($StP{$self->byte_size}, 0), # Start of free chain (blist size)
            pack($StP{$self->byte_size}, 0), # Start of free chain (data size)
            pack($StP{$self->byte_size}, 0), # Start of free chain (index size)
        );

        #XXX Set these less fragilely
        $self->set_trans_loc( $header_fixed + 4 );
        $self->set_chains_loc( $header_fixed + 4 + $bl + $STALE_SIZE * ($nt-1) );

        return;
    }

    sub _read_file_header {
        my $self = shift;

        my $buffer = $self->storage->read_at( 0, $header_fixed );
        return unless length($buffer);

        my ($file_signature, $sig_header, $file_version, $size) = unpack(
            'A4 A N N', $buffer
        );

        unless ( $file_signature eq SIG_FILE ) {
            $self->storage->close;
            DBM::Deep->_throw_error( "Signature not found -- file is not a Deep DB" );
        }

        unless ( $sig_header eq SIG_HEADER ) {
            $self->storage->close;
            DBM::Deep->_throw_error( "Pre-1.00 file version found" );
        }

        unless ( $file_version == $this_file_version ) {
            $self->storage->close;
            DBM::Deep->_throw_error(
                "Wrong file version found - " .  $file_version .
                " - expected " . $this_file_version
            );
        }

        my $buffer2 = $self->storage->read_at( undef, $size );
        my @values = unpack( 'C C C C', $buffer2 );

        if ( @values != 4 || grep { !defined } @values ) {
            $self->storage->close;
            DBM::Deep->_throw_error("Corrupted file - bad header");
        }

        #XXX Add warnings if values weren't set right
        @{$self}{qw(byte_size max_buckets data_sector_size num_txns)} = @values;

        # These shenangians are to allow a 256 within a C
        $self->{max_buckets} += 1;
        $self->{data_sector_size} += 1;

        my $bl = $self->txn_bitfield_len;

        my $header_var = scalar(@values) + $bl + $STALE_SIZE * ($self->num_txns - 1) + 3 * $self->byte_size;
        unless ( $size == $header_var ) {
            $self->storage->close;
            DBM::Deep->_throw_error( "Unexpected size found ($size <-> $header_var)." );
        }

        $self->set_trans_loc( $header_fixed + scalar(@values) );
        $self->set_chains_loc( $header_fixed + scalar(@values) + $bl + $STALE_SIZE * ($self->num_txns - 1) );

        return length($buffer) + length($buffer2);
    }
}

=head2 _load_sector( $offset )

This will instantiate and return the sector object that represents the data found
at $offset.

=cut

sub _load_sector {
    my $self = shift;
    my ($offset) = @_;

    # Add a catch for offset of 0 or 1
    return if !$offset || $offset <= 1;

    my $type = $self->storage->read_at( $offset, 1 );
    return if $type eq chr(0);

    if ( $type eq $self->SIG_ARRAY || $type eq $self->SIG_HASH ) {
        return DBM::Deep::Engine::Sector::Reference->new({
            engine => $self,
            type   => $type,
            offset => $offset,
        });
    }
    # XXX Don't we need key_md5 here?
    elsif ( $type eq $self->SIG_BLIST ) {
        return DBM::Deep::Engine::Sector::BucketList->new({
            engine => $self,
            type   => $type,
            offset => $offset,
        });
    }
    elsif ( $type eq $self->SIG_INDEX ) {
        return DBM::Deep::Engine::Sector::Index->new({
            engine => $self,
            type   => $type,
            offset => $offset,
        });
    }
    elsif ( $type eq $self->SIG_NULL ) {
        return DBM::Deep::Engine::Sector::Null->new({
            engine => $self,
            type   => $type,
            offset => $offset,
        });
    }
    elsif ( $type eq $self->SIG_DATA ) {
        return DBM::Deep::Engine::Sector::Scalar->new({
            engine => $self,
            type   => $type,
            offset => $offset,
        });
    }
    # This was deleted from under us, so just return and let the caller figure it out.
    elsif ( $type eq $self->SIG_FREE ) {
        return;
    }

    DBM::Deep->_throw_error( "'$offset': Don't know what to do with type '$type'" );
}

=head2 _apply_digest( @stuff )

This will apply the digest methd (default to Digest::MD5::md5) to the arguments
passed in and return the result.

=cut

sub _apply_digest {
    my $self = shift;
    return $self->{digest}->(@_);
}

=head2 _add_free_blist_sector( $offset, $size )

=head2 _add_free_data_sector( $offset, $size )

=head2 _add_free_index_sector( $offset, $size )

These methods are all wrappers around _add_free_sector(), providing the proper
chain offset ($multiple) for the sector type.

=cut

sub _add_free_blist_sector { shift->_add_free_sector( 0, @_ ) }
sub _add_free_data_sector { shift->_add_free_sector( 1, @_ ) }
sub _add_free_index_sector { shift->_add_free_sector( 2, @_ ) }

=head2 _add_free_sector( $multiple, $offset, $size )

_add_free_sector() takes the offset into the chains location, the offset of the
sector, and the size of that sector. It will mark the sector as a free sector
and put it into the list of sectors that are free of this type for use later.

This returns nothing.

B<NOTE>: $size is unused?

=cut

sub _add_free_sector {
    my $self = shift;
    my ($multiple, $offset, $size) = @_;

    my $chains_offset = $multiple * $self->byte_size;

    my $storage = $self->storage;

    # Increment staleness.
    # XXX Can this increment+modulo be done by "&= 0x1" ?
    my $staleness = unpack( $StP{$STALE_SIZE}, $storage->read_at( $offset + SIG_SIZE, $STALE_SIZE ) );
    $staleness = ($staleness + 1 ) % ( 2 ** ( 8 * $STALE_SIZE ) );
    $storage->print_at( $offset + SIG_SIZE, pack( $StP{$STALE_SIZE}, $staleness ) );

    my $old_head = $storage->read_at( $self->chains_loc + $chains_offset, $self->byte_size );

    $storage->print_at( $self->chains_loc + $chains_offset,
        pack( $StP{$self->byte_size}, $offset ),
    );

    # Record the old head in the new sector after the signature and staleness counter
    $storage->print_at( $offset + SIG_SIZE + $STALE_SIZE, $old_head );
}

=head2 _request_blist_sector( $size )

=head2 _request_data_sector( $size )

=head2 _request_index_sector( $size )

These methods are all wrappers around _request_sector(), providing the proper
chain offset ($multiple) for the sector type.

=cut

sub _request_blist_sector { shift->_request_sector( 0, @_ ) }
sub _request_data_sector { shift->_request_sector( 1, @_ ) }
sub _request_index_sector { shift->_request_sector( 2, @_ ) }

=head2 _request_sector( $multiple $size )

This takes the offset into the chains location and the size of that sector.

This returns the object with the sector. If there is an available free sector of
that type, then it will be reused. If there isn't one, then a new one will be
allocated.

=cut

sub _request_sector {
    my $self = shift;
    my ($multiple, $size) = @_;

    my $chains_offset = $multiple * $self->byte_size;

    my $old_head = $self->storage->read_at( $self->chains_loc + $chains_offset, $self->byte_size );
    my $loc = unpack( $StP{$self->byte_size}, $old_head );

    # We don't have any free sectors of the right size, so allocate a new one.
    unless ( $loc ) {
        my $offset = $self->storage->request_space( $size );

        # Zero out the new sector. This also guarantees correct increases
        # in the filesize.
        $self->storage->print_at( $offset, chr(0) x $size );

        return $offset;
    }

    # Read the new head after the signature and the staleness counter
    my $new_head = $self->storage->read_at( $loc + SIG_SIZE + $STALE_SIZE, $self->byte_size );
    $self->storage->print_at( $self->chains_loc + $chains_offset, $new_head );
    $self->storage->print_at(
        $loc + SIG_SIZE + $STALE_SIZE,
        pack( $StP{$self->byte_size}, 0 ),
    );

    return $loc;
}

=head2 flush()

This takes no arguments. It will do everything necessary to flush all things to
disk. This is usually called during unlock() and setup_fh().

This returns nothing.

=cut

sub flush {
    my $self = shift;

    # Why do we need to have the storage flush? Shouldn't autoflush take care of things?
    # -RobK, 2008-06-26
    $self->storage->flush;
}

=head2 ACCESSORS

The following are readonly attributes.

=over 4

=item * storage

=item * byte_size

=item * hash_size

=item * hash_chars

=item * num_txns

=item * max_buckets

=item * blank_md5

=item * data_sector_size

=item * txn_bitfield_len

=back

=cut

sub storage     { $_[0]{storage} }
sub byte_size   { $_[0]{byte_size} }
sub hash_size   { $_[0]{hash_size} }
sub hash_chars  { $_[0]{hash_chars} }
sub num_txns    { $_[0]{num_txns} }
sub max_buckets { $_[0]{max_buckets} }
sub blank_md5   { chr(0) x $_[0]->hash_size }
sub data_sector_size { $_[0]{data_sector_size} }

# This is a calculated value
sub txn_bitfield_len {
    my $self = shift;
    unless ( exists $self->{txn_bitfield_len} ) {
        my $temp = ($self->num_txns) / 8;
        if ( $temp > int( $temp ) ) {
            $temp = int( $temp ) + 1;
        }
        $self->{txn_bitfield_len} = $temp;
    }
    return $self->{txn_bitfield_len};
}

=pod

The following are read/write attributes. 

=over 4

=item * trans_id / set_trans_id( $new_id )

=item * trans_loc / set_trans_loc( $new_loc )

=item * chains_loc / set_chains_loc( $new_loc )

=back

=cut

sub trans_id     { $_[0]{trans_id} }
sub set_trans_id { $_[0]{trans_id} = $_[1] }

sub trans_loc     { $_[0]{trans_loc} }
sub set_trans_loc { $_[0]{trans_loc} = $_[1] }

sub chains_loc     { $_[0]{chains_loc} }
sub set_chains_loc { $_[0]{chains_loc} = $_[1] }

sub cache       { $_[0]{cache} ||= {} }
sub clear_cache { %{$_[0]->cache} = () }

=head2 _dump_file()

This method takes no arguments. It's used to print out a textual representation of the DBM::Deep
DB file. It assumes the file is not-corrupted.

=cut

sub _dump_file {
    my $self = shift;

    # Read the header
    my $spot = $self->_read_file_header();

    my %types = (
        0 => 'B',
        1 => 'D',
        2 => 'I',
    );

    my %sizes = (
        'D' => $self->data_sector_size,
        'B' => DBM::Deep::Engine::Sector::BucketList->new({engine=>$self,offset=>1})->size,
        'I' => DBM::Deep::Engine::Sector::Index->new({engine=>$self,offset=>1})->size,
    );

    my $return = "";

    # Header values
    $return .= "NumTxns: " . $self->num_txns . $/;

    # Read the free sector chains
    my %sectors;
    foreach my $multiple ( 0 .. 2 ) {
        $return .= "Chains($types{$multiple}):";
        my $old_loc = $self->chains_loc + $multiple * $self->byte_size;
        while ( 1 ) {
            my $loc = unpack(
                $StP{$self->byte_size},
                $self->storage->read_at( $old_loc, $self->byte_size ),
            );

            # We're now out of free sectors of this kind.
            unless ( $loc ) {
                last;
            }

            $sectors{ $types{$multiple} }{ $loc } = undef;
            $old_loc = $loc + SIG_SIZE + $STALE_SIZE;
            $return .= " $loc";
        }
        $return .= $/;
    }

    SECTOR:
    while ( $spot < $self->storage->{end} ) {
        # Read each sector in order.
        my $sector = $self->_load_sector( $spot );
        if ( !$sector ) {
            # Find it in the free-sectors that were found already
            foreach my $type ( keys %sectors ) {
                if ( exists $sectors{$type}{$spot} ) {
                    my $size = $sizes{$type};
                    $return .= sprintf "%08d: %s %04d\n", $spot, 'F' . $type, $size;
                    $spot += $size;
                    next SECTOR;
                }
            }

            die "********\n$return\nDidn't find free sector for $spot in chains\n********\n";
        }
        else {
            $return .= sprintf "%08d: %s  %04d", $spot, $sector->type, $sector->size;
            if ( $sector->type eq 'D' ) {
                $return .= ' ' . $sector->data;
            }
            elsif ( $sector->type eq 'A' || $sector->type eq 'H' ) {
                $return .= ' REF: ' . $sector->get_refcount;
            }
            elsif ( $sector->type eq 'B' ) {
                foreach my $bucket ( $sector->chopped_up ) {
                    $return .= "\n    ";
                    $return .= sprintf "%08d", unpack($StP{$self->byte_size},
                        substr( $bucket->[-1], $self->hash_size, $self->byte_size),
                    );
                    my $l = unpack( $StP{$self->byte_size},
                        substr( $bucket->[-1],
                            $self->hash_size + $self->byte_size,
                            $self->byte_size,
                        ),
                    );
                    $return .= sprintf " %08d", $l;
                    foreach my $txn ( 0 .. $self->num_txns - 2 ) {
                        my $l = unpack( $StP{$self->byte_size},
                            substr( $bucket->[-1],
                                $self->hash_size + 2 * $self->byte_size + $txn * ($self->byte_size + $STALE_SIZE),
                                $self->byte_size,
                            ),
                        );
                        $return .= sprintf " %08d", $l;
                    }
                }
            }
            $return .= $/;

            $spot += $sector->size;
        }
    }

    return $return;
}

1;
__END__
