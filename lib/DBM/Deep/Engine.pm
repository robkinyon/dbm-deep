package DBM::Deep::Engine;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Engine::Sector::BucketList;
use DBM::Deep::Engine::Sector::Index;
use DBM::Deep::Engine::Sector::Null;
use DBM::Deep::Engine::Sector::Reference;
use DBM::Deep::Engine::Sector::Scalar;
use DBM::Deep::Iterator;

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

our $STALE_SIZE = 2;

# Please refer to the pack() documentation for further information
my %StP = (
    1 => 'C', # Unsigned char value (no order needed as it's just one byte)
    2 => 'n', # Unsigned short in "network" (big-endian) order
    4 => 'N', # Unsigned long in "network" (big-endian) order
    8 => 'Q', # Usigned quad (no order specified, presumably machine-dependent)
);
sub StP { $StP{$_[1]} }

################################################################################

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

################################################################################

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

sub make_reference {
    my $self = shift;
    my ($obj, $old_key, $new_key) = @_;

    # This will be a Reference sector
    my $sector = $self->_load_sector( $obj->_base_offset )
        or DBM::Deep->_throw_error( "How did get_classname fail (no sector for '$obj')?!" );

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
}

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

################################################################################

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

        $self->storage->set_inode;
    }

    return 1;
}

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

sub write_txn_slots {
    my $self = shift;
    my $num_bits = $self->txn_bitfield_len * 8;
    $self->storage->print_at( $self->trans_loc,
        pack( 'b'.$num_bits, join('', @_) ),
    );
}

sub get_running_txn_ids {
    my $self = shift;
    my @transactions = $self->read_txn_slots;
    my @trans_ids = map { $_+1} grep { $transactions[$_] } 0 .. $#transactions;
}

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

sub get_entries {
    my $self = shift;
    return [ keys %{ $self->{entries}{$self->trans_id} ||= {} } ];
}

sub add_entry {
    my $self = shift;
    my ($trans_id, $loc) = @_;

    $self->{entries}{$trans_id} ||= {};
    $self->{entries}{$trans_id}{$loc} = undef;
}

# If the buckets are being relocated because of a reindexing, the entries
# mechanism needs to be made aware of it.
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

sub clear_entries {
    my $self = shift;
    delete $self->{entries}{$self->trans_id};
}

################################################################################

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

sub _apply_digest {
    my $self = shift;
    return $self->{digest}->(@_);
}

sub _add_free_blist_sector { shift->_add_free_sector( 0, @_ ) }
sub _add_free_data_sector { shift->_add_free_sector( 1, @_ ) }
sub _add_free_index_sector { shift->_add_free_sector( 2, @_ ) }

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

sub _request_blist_sector { shift->_request_sector( 0, @_ ) }
sub _request_data_sector { shift->_request_sector( 1, @_ ) }
sub _request_index_sector { shift->_request_sector( 2, @_ ) }

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

################################################################################

sub lock_exclusive {
    my $self = shift;
    my ($obj) = @_;
    return $self->storage->lock_exclusive( $obj );
}

sub lock_shared {
    my $self = shift;
    my ($obj) = @_;
    return $self->storage->lock_shared( $obj );
}

sub unlock {
    my $self = shift;
    my ($obj) = @_;
    return $self->storage->unlock( $obj );
}

################################################################################

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

sub trans_id     { $_[0]{trans_id} }
sub set_trans_id { $_[0]{trans_id} = $_[1] }

sub trans_loc     { $_[0]{trans_loc} }
sub set_trans_loc { $_[0]{trans_loc} = $_[1] }

sub chains_loc     { $_[0]{chains_loc} }
sub set_chains_loc { $_[0]{chains_loc} = $_[1] }

sub cache       { $_[0]{cache} ||= {} }
sub clear_cache { %{$_[0]->cache} = () }

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
