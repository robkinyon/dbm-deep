package DBM::Deep::Engine;

use 5.006_000;

use strict;
use warnings;

our $VERSION = q(1.0012);

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

my $STALE_SIZE = 2;

# Please refer to the pack() documentation for further information
my %StP = (
    1 => 'C', # Unsigned char value (no order needed as it's just one byte)
    2 => 'n', # Unsigned short in "network" (big-endian) order
    4 => 'N', # Unsigned long in "network" (big-endian) order
    8 => 'Q', # Usigned quad (no order specified, presumably machine-dependent)
);

################################################################################

sub new {
    my $class = shift;
    my ($args) = @_;

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
        foreach my $orig_loc ( keys %{ $locs } ) {
            if ( $orig_loc == $old_loc ) {
                delete $locs->{orig_loc};
                $locs->{$new_loc} = undef;
                next TRANS;
            }
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

################################################################################

package DBM::Deep::Iterator;

sub new {
    my $class = shift;
    my ($args) = @_;

    my $self = bless {
        breadcrumbs => [],
        engine      => $args->{engine},
        base_offset => $args->{base_offset},
    }, $class;

    Scalar::Util::weaken( $self->{engine} );

    return $self;
}

sub reset { $_[0]{breadcrumbs} = [] }

sub get_sector_iterator {
    my $self = shift;
    my ($loc) = @_;

    my $sector = $self->{engine}->_load_sector( $loc )
        or return;

    if ( $sector->isa( 'DBM::Deep::Engine::Sector::Index' ) ) {
        return DBM::Deep::Iterator::Index->new({
            iterator => $self,
            sector   => $sector,
        });
    }
    elsif ( $sector->isa( 'DBM::Deep::Engine::Sector::BucketList' ) ) {
        return DBM::Deep::Iterator::BucketList->new({
            iterator => $self,
            sector   => $sector,
        });
    }

    DBM::Deep->_throw_error( "get_sector_iterator(): Why did $loc make a $sector?" );
}

sub get_next_key {
    my $self = shift;
    my ($obj) = @_;

    my $crumbs = $self->{breadcrumbs};
    my $e = $self->{engine};

    unless ( @$crumbs ) {
        # This will be a Reference sector
        my $sector = $e->_load_sector( $self->{base_offset} )
            # If no sector is found, thist must have been deleted from under us.
            or return;

        if ( $sector->staleness != $obj->_staleness ) {
            return;
        }

        my $loc = $sector->get_blist_loc
            or return;

        push @$crumbs, $self->get_sector_iterator( $loc );
    }

    FIND_NEXT_KEY: {
        # We're at the end.
        unless ( @$crumbs ) {
            $self->reset;
            return;
        }

        my $iterator = $crumbs->[-1];

        # This level is done.
        if ( $iterator->at_end ) {
            pop @$crumbs;
            redo FIND_NEXT_KEY;
        }

        if ( $iterator->isa( 'DBM::Deep::Iterator::Index' ) ) {
            # If we don't have any more, it will be caught at the
            # prior check.
            if ( my $next = $iterator->get_next_iterator ) {
                push @$crumbs, $next;
            }
            redo FIND_NEXT_KEY;
        }

        unless ( $iterator->isa( 'DBM::Deep::Iterator::BucketList' ) ) {
            DBM::Deep->_throw_error(
                "Should have a bucketlist iterator here - instead have $iterator"
            );
        }

        # At this point, we have a BucketList iterator
        my $key = $iterator->get_next_key;
        if ( defined $key ) {
            return $key;
        }
        #XXX else { $iterator->set_to_end() } ?

        # We hit the end of the bucketlist iterator, so redo
        redo FIND_NEXT_KEY;
    }

    DBM::Deep->_throw_error( "get_next_key(): How did we get here?" );
}

package DBM::Deep::Iterator::Index;

sub new {
    my $self = bless $_[1] => $_[0];
    $self->{curr_index} = 0;
    return $self;
}

sub at_end {
    my $self = shift;
    return $self->{curr_index} >= $self->{iterator}{engine}->hash_chars;
}

sub get_next_iterator {
    my $self = shift;

    my $loc;
    while ( !$loc ) {
        return if $self->at_end;
        $loc = $self->{sector}->get_entry( $self->{curr_index}++ );
    }

    return $self->{iterator}->get_sector_iterator( $loc );
}

package DBM::Deep::Iterator::BucketList;

sub new {
    my $self = bless $_[1] => $_[0];
    $self->{curr_index} = 0;
    return $self;
}

sub at_end {
    my $self = shift;
    return $self->{curr_index} >= $self->{iterator}{engine}->max_buckets;
}

sub get_next_key {
    my $self = shift;

    return if $self->at_end;

    my $idx = $self->{curr_index}++;

    my $data_loc = $self->{sector}->get_data_location_for({
        allow_head => 1,
        idx        => $idx,
    }) or return;

    #XXX Do we want to add corruption checks here?
    return $self->{sector}->get_key_for( $idx )->data;
}

package DBM::Deep::Engine::Sector;

sub new {
    my $self = bless $_[1], $_[0];
    Scalar::Util::weaken( $self->{engine} );
    $self->_init;
    return $self;
}

#sub _init {}
#sub clone { DBM::Deep->_throw_error( "Must be implemented in the child class" ); }

sub engine { $_[0]{engine} }
sub offset { $_[0]{offset} }
sub type   { $_[0]{type} }

sub base_size {
   my $self = shift;
   return $self->engine->SIG_SIZE + $STALE_SIZE;
}

sub free {
    my $self = shift;

    my $e = $self->engine;

    $e->storage->print_at( $self->offset, $e->SIG_FREE );
    # Skip staleness counter
    $e->storage->print_at( $self->offset + $self->base_size,
        chr(0) x ($self->size - $self->base_size),
    );

    my $free_meth = $self->free_meth;
    $e->$free_meth( $self->offset, $self->size );

    return;
}

package DBM::Deep::Engine::Sector::Data;

our @ISA = qw( DBM::Deep::Engine::Sector );

# This is in bytes
sub size { $_[0]{engine}->data_sector_size }
sub free_meth { return '_add_free_data_sector' }

sub clone {
    my $self = shift;
    return ref($self)->new({
        engine => $self->engine,
        type   => $self->type,
        data   => $self->data,
    });
}

package DBM::Deep::Engine::Sector::Scalar;

our @ISA = qw( DBM::Deep::Engine::Sector::Data );

sub free {
    my $self = shift;

    my $chain_loc = $self->chain_loc;

    $self->SUPER::free();

    if ( $chain_loc ) {
        $self->engine->_load_sector( $chain_loc )->free;
    }

    return;
}

sub type { $_[0]{engine}->SIG_DATA }
sub _init {
    my $self = shift;

    my $engine = $self->engine;

    unless ( $self->offset ) {
        my $data_section = $self->size - $self->base_size - $engine->byte_size - 1;

        $self->{offset} = $engine->_request_data_sector( $self->size );

        my $data = delete $self->{data};
        my $dlen = length $data;
        my $continue = 1;
        my $curr_offset = $self->offset;
        while ( $continue ) {

            my $next_offset = 0;

            my ($leftover, $this_len, $chunk);
            if ( $dlen > $data_section ) {
                $leftover = 0;
                $this_len = $data_section;
                $chunk = substr( $data, 0, $this_len );

                $dlen -= $data_section;
                $next_offset = $engine->_request_data_sector( $self->size );
                $data = substr( $data, $this_len );
            }
            else {
                $leftover = $data_section - $dlen;
                $this_len = $dlen;
                $chunk = $data;

                $continue = 0;
            }

            $engine->storage->print_at( $curr_offset, $self->type ); # Sector type
            # Skip staleness
            $engine->storage->print_at( $curr_offset + $self->base_size,
                pack( $StP{$engine->byte_size}, $next_offset ),  # Chain loc
                pack( $StP{1}, $this_len ),                      # Data length
                $chunk,                                          # Data to be stored in this sector
                chr(0) x $leftover,                              # Zero-fill the rest
            );

            $curr_offset = $next_offset;
        }

        return;
    }
}

sub data_length {
    my $self = shift;

    my $buffer = $self->engine->storage->read_at(
        $self->offset + $self->base_size + $self->engine->byte_size, 1
    );

    return unpack( $StP{1}, $buffer );
}

sub chain_loc {
    my $self = shift;
    return unpack(
        $StP{$self->engine->byte_size},
        $self->engine->storage->read_at(
            $self->offset + $self->base_size,
            $self->engine->byte_size,
        ),
    );
}

sub data {
    my $self = shift;
#    my ($args) = @_;
#    $args ||= {};

    my $data;
    while ( 1 ) {
        my $chain_loc = $self->chain_loc;

        $data .= $self->engine->storage->read_at(
            $self->offset + $self->base_size + $self->engine->byte_size + 1, $self->data_length,
        );

        last unless $chain_loc;

        $self = $self->engine->_load_sector( $chain_loc );
    }

    return $data;
}

package DBM::Deep::Engine::Sector::Null;

our @ISA = qw( DBM::Deep::Engine::Sector::Data );

sub type { $_[0]{engine}->SIG_NULL }
sub data_length { 0 }
sub data { return }

sub _init {
    my $self = shift;

    my $engine = $self->engine;

    unless ( $self->offset ) {
        my $leftover = $self->size - $self->base_size - 1 * $engine->byte_size - 1;

        $self->{offset} = $engine->_request_data_sector( $self->size );
        $engine->storage->print_at( $self->offset, $self->type ); # Sector type
        # Skip staleness counter
        $engine->storage->print_at( $self->offset + $self->base_size,
            pack( $StP{$engine->byte_size}, 0 ),  # Chain loc
            pack( $StP{1}, $self->data_length ),  # Data length
            chr(0) x $leftover,                   # Zero-fill the rest
        );

        return;
    }
}

package DBM::Deep::Engine::Sector::Reference;

our @ISA = qw( DBM::Deep::Engine::Sector::Data );

sub _init {
    my $self = shift;

    my $e = $self->engine;

    unless ( $self->offset ) {
        my $classname = Scalar::Util::blessed( delete $self->{data} );
        my $leftover = $self->size - $self->base_size - 3 * $e->byte_size;

        my $class_offset = 0;
        if ( defined $classname ) {
            my $class_sector = DBM::Deep::Engine::Sector::Scalar->new({
                engine => $e,
                data   => $classname,
            });
            $class_offset = $class_sector->offset;
        }

        $self->{offset} = $e->_request_data_sector( $self->size );
        $e->storage->print_at( $self->offset, $self->type ); # Sector type
        # Skip staleness counter
        $e->storage->print_at( $self->offset + $self->base_size,
            pack( $StP{$e->byte_size}, 0 ),             # Index/BList loc
            pack( $StP{$e->byte_size}, $class_offset ), # Classname loc
            pack( $StP{$e->byte_size}, 1 ),             # Initial refcount
            chr(0) x $leftover,                         # Zero-fill the rest
        );
    }
    else {
        $self->{type} = $e->storage->read_at( $self->offset, 1 );
    }

    $self->{staleness} = unpack(
        $StP{$STALE_SIZE},
        $e->storage->read_at( $self->offset + $e->SIG_SIZE, $STALE_SIZE ),
    );

    return;
}

sub staleness { $_[0]{staleness} }

sub get_data_location_for {
    my $self = shift;
    my ($args) = @_;

    # Assume that the head is not allowed unless otherwise specified.
    $args->{allow_head} = 0 unless exists $args->{allow_head};

    # Assume we don't create a new blist location unless otherwise specified.
    $args->{create} = 0 unless exists $args->{create};

    my $blist = $self->get_bucket_list({
        key_md5 => $args->{key_md5},
        key => $args->{key},
        create  => $args->{create},
    });
    return unless $blist && $blist->{found};

    # At this point, $blist knows where the md5 is. What it -doesn't- know yet
    # is whether or not this transaction has this key. That's part of the next
    # function call.
    my $location = $blist->get_data_location_for({
        allow_head => $args->{allow_head},
    }) or return;

    return $location;
}

sub get_data_for {
    my $self = shift;
    my ($args) = @_;

    my $location = $self->get_data_location_for( $args )
        or return;

    return $self->engine->_load_sector( $location );
}

sub write_data {
    my $self = shift;
    my ($args) = @_;

    my $blist = $self->get_bucket_list({
        key_md5 => $args->{key_md5},
        key => $args->{key},
        create  => 1,
    }) or DBM::Deep->_throw_error( "How did write_data fail (no blist)?!" );

    # Handle any transactional bookkeeping.
    if ( $self->engine->trans_id ) {
        if ( ! $blist->has_md5 ) {
            $blist->mark_deleted({
                trans_id => 0,
            });
        }
    }
    else {
        my @trans_ids = $self->engine->get_running_txn_ids;
        if ( $blist->has_md5 ) {
            if ( @trans_ids ) {
                my $old_value = $blist->get_data_for;
                foreach my $other_trans_id ( @trans_ids ) {
                    next if $blist->get_data_location_for({
                        trans_id   => $other_trans_id,
                        allow_head => 0,
                    });
                    $blist->write_md5({
                        trans_id => $other_trans_id,
                        key      => $args->{key},
                        key_md5  => $args->{key_md5},
                        value    => $old_value->clone,
                    });
                }
            }
        }
        else {
            if ( @trans_ids ) {
                foreach my $other_trans_id ( @trans_ids ) {
                    #XXX This doesn't seem to possible to ever happen . . .
                    next if $blist->get_data_location_for({ trans_id => $other_trans_id, allow_head => 0 });
                    $blist->mark_deleted({
                        trans_id => $other_trans_id,
                    });
                }
            }
        }
    }

    #XXX Is this safe to do transactionally?
    # Free the place we're about to write to.
    if ( $blist->get_data_location_for({ allow_head => 0 }) ) {
        $blist->get_data_for({ allow_head => 0 })->free;
    }

    $blist->write_md5({
        key      => $args->{key},
        key_md5  => $args->{key_md5},
        value    => $args->{value},
    });
}

sub delete_key {
    my $self = shift;
    my ($args) = @_;

    # XXX What should happen if this fails?
    my $blist = $self->get_bucket_list({
        key_md5 => $args->{key_md5},
    }) or DBM::Deep->_throw_error( "How did delete_key fail (no blist)?!" );

    # Save the location so that we can free the data
    my $location = $blist->get_data_location_for({
        allow_head => 0,
    });
    my $old_value = $location && $self->engine->_load_sector( $location );

    my @trans_ids = $self->engine->get_running_txn_ids;

    # If we're the HEAD and there are running txns, then we need to clone this value to the other
    # transactions to preserve Isolation.
    if ( $self->engine->trans_id == 0 ) {
        if ( @trans_ids ) {
            foreach my $other_trans_id ( @trans_ids ) {
                next if $blist->get_data_location_for({ trans_id => $other_trans_id, allow_head => 0 });
                $blist->write_md5({
                    trans_id => $other_trans_id,
                    key      => $args->{key},
                    key_md5  => $args->{key_md5},
                    value    => $old_value->clone,
                });
            }
        }
    }

    my $data;
    if ( @trans_ids ) {
        $blist->mark_deleted( $args );

        if ( $old_value ) {
            $data = $old_value->data({ export => 1 });
            $old_value->free;
        }
    }
    else {
        $data = $blist->delete_md5( $args );
    }

    return $data;
}

sub get_blist_loc {
    my $self = shift;

    my $e = $self->engine;
    my $blist_loc = $e->storage->read_at( $self->offset + $self->base_size, $e->byte_size );
    return unpack( $StP{$e->byte_size}, $blist_loc );
}

sub get_bucket_list {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    # XXX Add in check here for recycling?

    my $engine = $self->engine;

    my $blist_loc = $self->get_blist_loc;

    # There's no index or blist yet
    unless ( $blist_loc ) {
        return unless $args->{create};

        my $blist = DBM::Deep::Engine::Sector::BucketList->new({
            engine  => $engine,
            key_md5 => $args->{key_md5},
        });

        $engine->storage->print_at( $self->offset + $self->base_size,
            pack( $StP{$engine->byte_size}, $blist->offset ),
        );

        return $blist;
    }

    my $sector = $engine->_load_sector( $blist_loc )
        or DBM::Deep->_throw_error( "Cannot read sector at $blist_loc in get_bucket_list()" );
    my $i = 0;
    my $last_sector = undef;
    while ( $sector->isa( 'DBM::Deep::Engine::Sector::Index' ) ) {
        $blist_loc = $sector->get_entry( ord( substr( $args->{key_md5}, $i++, 1 ) ) );
        $last_sector = $sector;
        if ( $blist_loc ) {
            $sector = $engine->_load_sector( $blist_loc )
                or DBM::Deep->_throw_error( "Cannot read sector at $blist_loc in get_bucket_list()" );
        }
        else {
            $sector = undef;
            last;
        }
    }

    # This means we went through the Index sector(s) and found an empty slot
    unless ( $sector ) {
        return unless $args->{create};

        DBM::Deep->_throw_error( "No last_sector when attempting to build a new entry" )
            unless $last_sector;

        my $blist = DBM::Deep::Engine::Sector::BucketList->new({
            engine  => $engine,
            key_md5 => $args->{key_md5},
        });

        $last_sector->set_entry( ord( substr( $args->{key_md5}, $i - 1, 1 ) ) => $blist->offset );

        return $blist;
    }

    $sector->find_md5( $args->{key_md5} );

    # See whether or not we need to reindex the bucketlist
    # Yes, the double-braces are there for a reason. if() doesn't create a redo-able block,
    # so we have to create a bare block within the if() for redo-purposes. Patch and idea
    # submitted by sprout@cpan.org. -RobK, 2008-01-09
    if ( !$sector->has_md5 && $args->{create} && $sector->{idx} == -1 ) {{
        my $redo;

        my $new_index = DBM::Deep::Engine::Sector::Index->new({
            engine => $engine,
        });

        my %blist_cache;
        #XXX q.v. the comments for this function.
        foreach my $entry ( $sector->chopped_up ) {
            my ($spot, $md5) = @{$entry};
            my $idx = ord( substr( $md5, $i, 1 ) );

            # XXX This is inefficient
            my $blist = $blist_cache{$idx}
                ||= DBM::Deep::Engine::Sector::BucketList->new({
                    engine => $engine,
                });

            $new_index->set_entry( $idx => $blist->offset );

            my $new_spot = $blist->write_at_next_open( $md5 );
            $engine->reindex_entry( $spot => $new_spot );
        }

        # Handle the new item separately.
        {
            my $idx = ord( substr( $args->{key_md5}, $i, 1 ) );

            # If all the previous blist's items have been thrown into one
            # blist and the new item belongs in there too, we need
            # another index.
            if ( keys %blist_cache == 1 and each %blist_cache == $idx ) {
                ++$i, ++$redo;
            } else {
                my $blist = $blist_cache{$idx}
                    ||= DBM::Deep::Engine::Sector::BucketList->new({
                        engine => $engine,
                    });
    
                $new_index->set_entry( $idx => $blist->offset );
    
                #XXX THIS IS HACKY!
                $blist->find_md5( $args->{key_md5} );
                $blist->write_md5({
                    key     => $args->{key},
                    key_md5 => $args->{key_md5},
                    value   => DBM::Deep::Engine::Sector::Null->new({
                        engine => $engine,
                        data   => undef,
                    }),
                });
            }
#            my $blist = $blist_cache{$idx}
#                ||= DBM::Deep::Engine::Sector::BucketList->new({
#                    engine => $engine,
#                });
#
#            $new_index->set_entry( $idx => $blist->offset );
#
#            #XXX THIS IS HACKY!
#            $blist->find_md5( $args->{key_md5} );
#            $blist->write_md5({
#                key     => $args->{key},
#                key_md5 => $args->{key_md5},
#                value   => DBM::Deep::Engine::Sector::Null->new({
#                    engine => $engine,
#                    data   => undef,
#                }),
#            });
        }

        if ( $last_sector ) {
            $last_sector->set_entry(
                ord( substr( $args->{key_md5}, $i - 1, 1 ) ),
                $new_index->offset,
            );
        } else {
            $engine->storage->print_at( $self->offset + $self->base_size,
                pack( $StP{$engine->byte_size}, $new_index->offset ),
            );
        }

        $sector->clear;
        $sector->free;

        if ( $redo ) {
            (undef, $sector) = %blist_cache;
            $last_sector = $new_index;
            redo;
        }

        $sector = $blist_cache{ ord( substr( $args->{key_md5}, $i, 1 ) ) };
        $sector->find_md5( $args->{key_md5} );
    }}

    return $sector;
}

sub get_class_offset {
    my $self = shift;

    my $e = $self->engine;
    return unpack(
        $StP{$e->byte_size},
        $e->storage->read_at(
            $self->offset + $self->base_size + 1 * $e->byte_size, $e->byte_size,
        ),
    );
}

sub get_classname {
    my $self = shift;

    my $class_offset = $self->get_class_offset;

    return unless $class_offset;

    return $self->engine->_load_sector( $class_offset )->data;
}

sub data {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    my $obj;
    unless ( $obj = $self->engine->cache->{ $self->offset } ) {
        $obj = DBM::Deep->new({
            type        => $self->type,
            base_offset => $self->offset,
            staleness   => $self->staleness,
            storage     => $self->engine->storage,
            engine      => $self->engine,
        });

        if ( $self->engine->storage->{autobless} ) {
            my $classname = $self->get_classname;
            if ( defined $classname ) {
                bless $obj, $classname;
            }
        }

        $self->engine->cache->{$self->offset} = $obj;
    }

    # We're not exporting, so just return.
    unless ( $args->{export} ) {
        return $obj;
    }

    # We shouldn't export if this is still referred to.
    if ( $self->get_refcount > 1 ) {
        return $obj;
    }

    return $obj->export;
}

sub free {
    my $self = shift;

    # We're not ready to be removed yet.
    if ( $self->decrement_refcount > 0 ) {
        return;
    }

    # Rebless the object into DBM::Deep::Null.
    eval { %{ $self->engine->cache->{ $self->offset } } = (); };
    eval { @{ $self->engine->cache->{ $self->offset } } = (); };
    bless $self->engine->cache->{ $self->offset }, 'DBM::Deep::Null';
    delete $self->engine->cache->{ $self->offset };

    my $blist_loc = $self->get_blist_loc;
    $self->engine->_load_sector( $blist_loc )->free if $blist_loc;

    my $class_loc = $self->get_class_offset;
    $self->engine->_load_sector( $class_loc )->free if $class_loc;

    $self->SUPER::free();
}

sub increment_refcount {
    my $self = shift;

    my $refcount = $self->get_refcount;

    $refcount++;

    $self->write_refcount( $refcount );

    return $refcount;
}

sub decrement_refcount {
    my $self = shift;

    my $refcount = $self->get_refcount;

    $refcount--;

    $self->write_refcount( $refcount );

    return $refcount;
}

sub get_refcount {
    my $self = shift;

    my $e = $self->engine;
    return unpack(
        $StP{$e->byte_size},
        $e->storage->read_at(
            $self->offset + $self->base_size + 2 * $e->byte_size, $e->byte_size,
        ),
    );
}

sub write_refcount {
    my $self = shift;
    my ($num) = @_;

    my $e = $self->engine;
    $e->storage->print_at(
        $self->offset + $self->base_size + 2 * $e->byte_size,
        pack( $StP{$e->byte_size}, $num ),
    );
}

package DBM::Deep::Engine::Sector::BucketList;

our @ISA = qw( DBM::Deep::Engine::Sector );

sub _init {
    my $self = shift;

    my $engine = $self->engine;

    unless ( $self->offset ) {
        my $leftover = $self->size - $self->base_size;

        $self->{offset} = $engine->_request_blist_sector( $self->size );
        $engine->storage->print_at( $self->offset, $engine->SIG_BLIST ); # Sector type
        # Skip staleness counter
        $engine->storage->print_at( $self->offset + $self->base_size,
            chr(0) x $leftover, # Zero-fill the data
        );
    }

    if ( $self->{key_md5} ) {
        $self->find_md5;
    }

    return $self;
}

sub clear {
    my $self = shift;
    $self->engine->storage->print_at( $self->offset + $self->base_size,
        chr(0) x ($self->size - $self->base_size), # Zero-fill the data
    );
}

sub size {
    my $self = shift;
    unless ( $self->{size} ) {
        my $e = $self->engine;
        # Base + numbuckets * bucketsize
        $self->{size} = $self->base_size + $e->max_buckets * $self->bucket_size;
    }
    return $self->{size};
}

sub free_meth { return '_add_free_blist_sector' }

sub free {
    my $self = shift;

    my $e = $self->engine;
    foreach my $bucket ( $self->chopped_up ) {
        my $rest = $bucket->[-1];

        # Delete the keysector
        my $l = unpack( $StP{$e->byte_size}, substr( $rest, $e->hash_size, $e->byte_size ) );
        my $s = $e->_load_sector( $l ); $s->free if $s;

        # Delete the HEAD sector
        $l = unpack( $StP{$e->byte_size},
            substr( $rest,
                $e->hash_size + $e->byte_size,
                $e->byte_size,
            ),
        );
        $s = $e->_load_sector( $l ); $s->free if $s;

        foreach my $txn ( 0 .. $e->num_txns - 2 ) {
            my $l = unpack( $StP{$e->byte_size},
                substr( $rest,
                    $e->hash_size + 2 * $e->byte_size + $txn * ($e->byte_size + $STALE_SIZE),
                    $e->byte_size,
                ),
            );
            my $s = $e->_load_sector( $l ); $s->free if $s;
        }
    }

    $self->SUPER::free();
}

sub bucket_size {
    my $self = shift;
    unless ( $self->{bucket_size} ) {
        my $e = $self->engine;
        # Key + head (location) + transactions (location + staleness-counter)
        my $location_size = $e->byte_size + $e->byte_size + ($e->num_txns - 1) * ($e->byte_size + $STALE_SIZE);
        $self->{bucket_size} = $e->hash_size + $location_size;
    }
    return $self->{bucket_size};
}

# XXX This is such a poor hack. I need to rethink this code.
sub chopped_up {
    my $self = shift;

    my $e = $self->engine;

    my @buckets;
    foreach my $idx ( 0 .. $e->max_buckets - 1 ) {
        my $spot = $self->offset + $self->base_size + $idx * $self->bucket_size;
        my $md5 = $e->storage->read_at( $spot, $e->hash_size );

        #XXX If we're chopping, why would we ever have the blank_md5?
        last if $md5 eq $e->blank_md5;

        my $rest = $e->storage->read_at( undef, $self->bucket_size - $e->hash_size );
        push @buckets, [ $spot, $md5 . $rest ];
    }

    return @buckets;
}

sub write_at_next_open {
    my $self = shift;
    my ($entry) = @_;

    #XXX This is such a hack!
    $self->{_next_open} = 0 unless exists $self->{_next_open};

    my $spot = $self->offset + $self->base_size + $self->{_next_open}++ * $self->bucket_size;
    $self->engine->storage->print_at( $spot, $entry );

    return $spot;
}

sub has_md5 {
    my $self = shift;
    unless ( exists $self->{found} ) {
        $self->find_md5;
    }
    return $self->{found};
}

sub find_md5 {
    my $self = shift;

    $self->{found} = undef;
    $self->{idx}   = -1;

    if ( @_ ) {
        $self->{key_md5} = shift;
    }

    # If we don't have an MD5, then what are we supposed to do?
    unless ( exists $self->{key_md5} ) {
        DBM::Deep->_throw_error( "Cannot find_md5 without a key_md5 set" );
    }

    my $e = $self->engine;
    foreach my $idx ( 0 .. $e->max_buckets - 1 ) {
        my $potential = $e->storage->read_at(
            $self->offset + $self->base_size + $idx * $self->bucket_size, $e->hash_size,
        );

        if ( $potential eq $e->blank_md5 ) {
            $self->{idx} = $idx;
            return;
        }

        if ( $potential eq $self->{key_md5} ) {
            $self->{found} = 1;
            $self->{idx} = $idx;
            return;
        }
    }

    return;
}

sub write_md5 {
    my $self = shift;
    my ($args) = @_;

    DBM::Deep->_throw_error( "write_md5: no key" ) unless exists $args->{key};
    DBM::Deep->_throw_error( "write_md5: no key_md5" ) unless exists $args->{key_md5};
    DBM::Deep->_throw_error( "write_md5: no value" ) unless exists $args->{value};

    my $engine = $self->engine;

    $args->{trans_id} = $engine->trans_id unless exists $args->{trans_id};

    my $spot = $self->offset + $self->base_size + $self->{idx} * $self->bucket_size;
    $engine->add_entry( $args->{trans_id}, $spot );

    unless ($self->{found}) {
        my $key_sector = DBM::Deep::Engine::Sector::Scalar->new({
            engine => $engine,
            data   => $args->{key},
        });

        $engine->storage->print_at( $spot,
            $args->{key_md5},
            pack( $StP{$engine->byte_size}, $key_sector->offset ),
        );
    }

    my $loc = $spot
      + $engine->hash_size
      + $engine->byte_size;

    if ( $args->{trans_id} ) {
        $loc += $engine->byte_size + ($args->{trans_id} - 1) * ( $engine->byte_size + $STALE_SIZE );

        $engine->storage->print_at( $loc,
            pack( $StP{$engine->byte_size}, $args->{value}->offset ),
            pack( $StP{$STALE_SIZE}, $engine->get_txn_staleness_counter( $args->{trans_id} ) ),
        );
    }
    else {
        $engine->storage->print_at( $loc,
            pack( $StP{$engine->byte_size}, $args->{value}->offset ),
        );
    }
}

sub mark_deleted {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    my $engine = $self->engine;

    $args->{trans_id} = $engine->trans_id unless exists $args->{trans_id};

    my $spot = $self->offset + $self->base_size + $self->{idx} * $self->bucket_size;
    $engine->add_entry( $args->{trans_id}, $spot );

    my $loc = $spot
      + $engine->hash_size
      + $engine->byte_size;

    if ( $args->{trans_id} ) {
        $loc += $engine->byte_size + ($args->{trans_id} - 1) * ( $engine->byte_size + $STALE_SIZE );

        $engine->storage->print_at( $loc,
            pack( $StP{$engine->byte_size}, 1 ), # 1 is the marker for deleted
            pack( $StP{$STALE_SIZE}, $engine->get_txn_staleness_counter( $args->{trans_id} ) ),
        );
    }
    else {
        $engine->storage->print_at( $loc,
            pack( $StP{$engine->byte_size}, 1 ), # 1 is the marker for deleted
        );
    }

}

sub delete_md5 {
    my $self = shift;
    my ($args) = @_;

    my $engine = $self->engine;
    return undef unless $self->{found};

    # Save the location so that we can free the data
    my $location = $self->get_data_location_for({
        allow_head => 0,
    });
    my $key_sector = $self->get_key_for;

    my $spot = $self->offset + $self->base_size + $self->{idx} * $self->bucket_size;
    $engine->storage->print_at( $spot,
        $engine->storage->read_at(
            $spot + $self->bucket_size,
            $self->bucket_size * ( $engine->max_buckets - $self->{idx} - 1 ),
        ),
        chr(0) x $self->bucket_size,
    );

    $key_sector->free;

    my $data_sector = $self->engine->_load_sector( $location );
    my $data = $data_sector->data({ export => 1 });
    $data_sector->free;

    return $data;
}

sub get_data_location_for {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    $args->{allow_head} = 0 unless exists $args->{allow_head};
    $args->{trans_id}   = $self->engine->trans_id unless exists $args->{trans_id};
    $args->{idx}        = $self->{idx} unless exists $args->{idx};

    my $e = $self->engine;

    my $spot = $self->offset + $self->base_size
      + $args->{idx} * $self->bucket_size
      + $e->hash_size
      + $e->byte_size;

    if ( $args->{trans_id} ) {
        $spot += $e->byte_size + ($args->{trans_id} - 1) * ( $e->byte_size + $STALE_SIZE );
    }

    my $buffer = $e->storage->read_at(
        $spot,
        $e->byte_size + $STALE_SIZE,
    );
    my ($loc, $staleness) = unpack( $StP{$e->byte_size} . ' ' . $StP{$STALE_SIZE}, $buffer );

    # XXX Merge the two if-clauses below
    if ( $args->{trans_id} ) {
        # We have found an entry that is old, so get rid of it
        if ( $staleness != (my $s = $e->get_txn_staleness_counter( $args->{trans_id} ) ) ) {
            $e->storage->print_at(
                $spot,
                pack( $StP{$e->byte_size} . ' ' . $StP{$STALE_SIZE}, (0) x 2 ), 
            );
            $loc = 0;
        }
    }

    # If we're in a transaction and we never wrote to this location, try the
    # HEAD instead.
    if ( $args->{trans_id} && !$loc && $args->{allow_head} ) {
        return $self->get_data_location_for({
            trans_id   => 0,
            allow_head => 1,
            idx        => $args->{idx},
        });
    }

    return $loc <= 1 ? 0 : $loc;
}

sub get_data_for {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    return unless $self->{found};
    my $location = $self->get_data_location_for({
        allow_head => $args->{allow_head},
    });
    return $self->engine->_load_sector( $location );
}

sub get_key_for {
    my $self = shift;
    my ($idx) = @_;
    $idx = $self->{idx} unless defined $idx;

    if ( $idx >= $self->engine->max_buckets ) {
        DBM::Deep->_throw_error( "get_key_for(): Attempting to retrieve $idx" );
    }

    my $location = $self->engine->storage->read_at(
        $self->offset + $self->base_size + $idx * $self->bucket_size + $self->engine->hash_size,
        $self->engine->byte_size,
    );
    $location = unpack( $StP{$self->engine->byte_size}, $location );
    DBM::Deep->_throw_error( "get_key_for: No location?" ) unless $location;

    return $self->engine->_load_sector( $location );
}

package DBM::Deep::Engine::Sector::Index;

our @ISA = qw( DBM::Deep::Engine::Sector );

sub _init {
    my $self = shift;

    my $engine = $self->engine;

    unless ( $self->offset ) {
        my $leftover = $self->size - $self->base_size;

        $self->{offset} = $engine->_request_index_sector( $self->size );
        $engine->storage->print_at( $self->offset, $engine->SIG_INDEX ); # Sector type
        # Skip staleness counter
        $engine->storage->print_at( $self->offset + $self->base_size,
            chr(0) x $leftover, # Zero-fill the rest
        );
    }

    return $self;
}

#XXX Change here
sub size {
    my $self = shift;
    unless ( $self->{size} ) {
        my $e = $self->engine;
        $self->{size} = $self->base_size + $e->byte_size * $e->hash_chars;
    }
    return $self->{size};
}

sub free_meth { return '_add_free_index_sector' }

sub free {
    my $self = shift;
    my $e = $self->engine;

    for my $i ( 0 .. $e->hash_chars - 1 ) {
        my $l = $self->get_entry( $i ) or next;
        $e->_load_sector( $l )->free;
    }

    $self->SUPER::free();
}

sub _loc_for {
    my $self = shift;
    my ($idx) = @_;
    return $self->offset + $self->base_size + $idx * $self->engine->byte_size;
}

sub get_entry {
    my $self = shift;
    my ($idx) = @_;

    my $e = $self->engine;

    DBM::Deep->_throw_error( "get_entry: Out of range ($idx)" )
        if $idx < 0 || $idx >= $e->hash_chars;

    return unpack(
        $StP{$e->byte_size},
        $e->storage->read_at( $self->_loc_for( $idx ), $e->byte_size ),
    );
}

sub set_entry {
    my $self = shift;
    my ($idx, $loc) = @_;

    my $e = $self->engine;

    DBM::Deep->_throw_error( "set_entry: Out of range ($idx)" )
        if $idx < 0 || $idx >= $e->hash_chars;

    $self->engine->storage->print_at(
        $self->_loc_for( $idx ),
        pack( $StP{$e->byte_size}, $loc ),
    );
}

# This was copied from MARCEL's Class::Null. However, I couldn't use it because
# I need an undef value, not an implementation of the Null Class pattern.
package DBM::Deep::Null;

use overload
    'bool'   => sub { undef },
    '""'     => sub { undef },
    '0+'     => sub { undef },
    fallback => 1,
    nomethod => 'AUTOLOAD';

sub AUTOLOAD { return; }

1;
__END__
