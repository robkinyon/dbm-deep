package DBM::Deep::Engine;

#use Sub::Caller qw( load_tag );

use 5.6.0;

use strict;

our $VERSION = q(0.99_03);

use Fcntl qw( :DEFAULT :flock );
use Scalar::Util ();

# File-wide notes:
# * To add to bucket_size, make sure you modify the following:
#   - calculate_sizes()
#   - _get_key_subloc()
#   - add_bucket() - where the buckets are printed
#
# * Every method in here assumes that the _storage has been appropriately
#   safeguarded. This can be anything from flock() to some sort of manual
#   mutex. But, it's the caller's responsability to make sure that this has
#   been done.

##
# Setup file and tag signatures.  These should never change.
##
sub SIG_FILE     () { 'DPDB' }
sub SIG_HEADER   () { 'h'    }
sub SIG_INTERNAL () { 'i'    }
sub SIG_HASH     () { 'H'    }
sub SIG_ARRAY    () { 'A'    }
sub SIG_NULL     () { 'N'    }
sub SIG_DATA     () { 'D'    }
sub SIG_INDEX    () { 'I'    }
sub SIG_BLIST    () { 'B'    }
sub SIG_FREE     () { 'F'    }
sub SIG_KEYS     () { 'K'    }
sub SIG_SIZE     () {  1     }

# This is the transaction ID for the HEAD
sub HEAD () { 0 }

################################################################################
#
# This is new code. It is a complete rewrite of the engine based on a new API
#
################################################################################

sub read_value {
    my $self = shift;
    my ($trans_id, $offset, $key, $orig_key) = @_;

    my $dig_key = $self->_apply_digest( $key );
    my $tag = $self->find_blist( $offset, $dig_key ) or return;
    return $self->get_bucket_value( $tag, $dig_key, $orig_key );
}

sub key_exists {
    my $self = shift;
    my ($trans_id, $offset, $key) = @_;

    my $dig_key = $self->_apply_digest( $key );
    # exists() returns the empty string, not undef
    my $tag = $self->find_blist( $offset, $dig_key ) or return '';
    return $self->bucket_exists( $tag, $dig_key, $key );
}

sub get_next_key {
    my $self = shift;
    my ($trans_id, $offset) = @_;

    # If the previous key was not specifed, start at the top and
    # return the first one found.
    my $temp;
    if ( @_ > 2 ) {
        $temp = {
            prev_md5    => $self->_apply_digest($_[2]),
            return_next => 0,
        };
    }
    else {
        $temp = {
            prev_md5    => chr(0) x $self->{hash_size},
            return_next => 1,
        };
    }

    return $self->traverse_index( $temp, $offset, 0 );
}

sub delete_key {
    my $self = shift;
    my ($trans_id, $offset, $key, $orig_key) = @_;

    my $dig_key = $self->_apply_digest( $key );
    my $tag = $self->find_blist( $offset, $dig_key ) or return;
    my $value = $self->get_bucket_value( $tag, $dig_key, $orig_key );
    $self->delete_bucket( $tag, $dig_key, $orig_key );
    return $value;
}

sub write_value {
    my $self = shift;
    my ($trans_id, $offset, $key, $value, $orig_key) = @_;

    my $dig_key = $self->_apply_digest( $key );
    my $tag = $self->find_blist( $offset, $dig_key, { create => 1 } );
    return $self->add_bucket( $tag, $dig_key, $key, $value, undef, $orig_key );
}

################################################################################
#
# Below here is the old code. It will be folded into the code above as it can.
#
################################################################################

sub new {
    my $class = shift;
    my ($args) = @_;

    my $self = bless {
        long_size => 4,
        long_pack => 'N',
        data_size => 4,
        data_pack => 'N',

        digest    => \&Digest::MD5::md5,
        hash_size => 16, # In bytes

        ##
        # Number of buckets per blist before another level of indexing is
        # done. Increase this value for slightly greater speed, but larger database
        # files. DO NOT decrease this value below 16, due to risk of recursive
        # reindex overrun.
        ##
        max_buckets => 16,

        storage => undef,
        obj     => undef,
    }, $class;

    if ( defined $args->{pack_size} ) {
        if ( lc $args->{pack_size} eq 'small' ) {
            $args->{long_size} = 2;
            $args->{long_pack} = 'n';
        }
        elsif ( lc $args->{pack_size} eq 'medium' ) {
            $args->{long_size} = 4;
            $args->{long_pack} = 'N';
        }
        elsif ( lc $args->{pack_size} eq 'large' ) {
            $args->{long_size} = 8;
            $args->{long_pack} = 'Q';
        }
        else {
            die "Unknown pack_size value: '$args->{pack_size}'\n";
        }
    }

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }
    Scalar::Util::weaken( $self->{obj} ) if $self->{obj};

    if ( $self->{max_buckets} < 16 ) {
        warn "Floor of max_buckets is 16. Setting it to 16 from '$self->{max_buckets}'\n";
        $self->{max_buckets} = 16;
    }

    return $self;
}

sub _storage { return $_[0]{storage} }

sub _apply_digest {
    my $self = shift;
    return $self->{digest}->(@_);
}

sub calculate_sizes {
    my $self = shift;

    # The 2**8 here indicates the number of different characters in the
    # current hashing algorithm
    #XXX Does this need to be updated with different hashing algorithms?
    $self->{hash_chars_used}  = (2**8);
    $self->{index_size}       = $self->{hash_chars_used} * $self->{long_size};

    $self->{bucket_size}      = $self->{hash_size} + $self->{long_size} * 2;
    $self->{bucket_list_size} = $self->{max_buckets} * $self->{bucket_size};

    $self->{key_size}         = $self->{long_size} * 2;
    $self->{keyloc_size}      = $self->{max_buckets} * $self->{key_size};

    return;
}

sub write_file_header {
    my $self = shift;

    my $loc = $self->_storage->request_space( length( SIG_FILE ) + 33 );

    $self->_storage->print_at( $loc,
        SIG_FILE,
        SIG_HEADER,
        pack('N', 1),  # header version
        pack('N', 24), # header size
        pack('N4', 0, 0, 0, 0),  # currently running transaction IDs
        pack('n', $self->{long_size}),
        pack('A', $self->{long_pack}),
        pack('n', $self->{data_size}),
        pack('A', $self->{data_pack}),
        pack('n', $self->{max_buckets}),
    );

    $self->_storage->set_transaction_offset( 13 );

    return;
}

sub read_file_header {
    my $self = shift;

    my $buffer = $self->_storage->read_at( 0, length(SIG_FILE) + 9 );
    return unless length($buffer);

    my ($file_signature, $sig_header, $header_version, $size) = unpack(
        'A4 A N N', $buffer
    );

    unless ( $file_signature eq SIG_FILE ) {
        $self->_storage->close;
        $self->_throw_error( "Signature not found -- file is not a Deep DB" );
    }

    unless ( $sig_header eq SIG_HEADER ) {
        $self->_storage->close;
        $self->_throw_error( "Old file version found." );
    }

    my $buffer2 = $self->_storage->read_at( undef, $size );
    my ($a1, $a2, $a3, $a4, @values) = unpack( 'N4 n A n A n', $buffer2 );

    $self->_storage->set_transaction_offset( 13 );

    if ( @values < 5 || grep { !defined } @values ) {
        $self->_storage->close;
        $self->_throw_error("Corrupted file - bad header");
    }

    #XXX Add warnings if values weren't set right
    @{$self}{qw(long_size long_pack data_size data_pack max_buckets)} = @values;

    return length($buffer) + length($buffer2);
}

sub setup_fh {
    my $self = shift;
    my ($obj) = @_;

    # Need to remove use of $fh here
    my $fh = $self->_storage->{fh};
    flock $fh, LOCK_EX;

    #XXX The duplication of calculate_sizes needs to go away
    unless ( $obj->{base_offset} ) {
        my $bytes_read = $self->read_file_header;

        $self->calculate_sizes;

        ##
        # File is empty -- write header and master index
        ##
        if (!$bytes_read) {
            $self->_storage->audit( "# Database created on" );

            $self->write_file_header;

            $obj->{base_offset} = $self->_storage->request_space(
                $self->tag_size( $self->{index_size} ),
            );

            $self->write_tag(
                $obj->_base_offset, $obj->_type,
                chr(0)x$self->{index_size},
            );

            # Flush the filehandle
            my $old_fh = select $fh;
            my $old_af = $|; $| = 1; $| = $old_af;
            select $old_fh;
        }
        else {
            $obj->{base_offset} = $bytes_read;

            ##
            # Get our type from master index header
            ##
            my $tag = $self->load_tag($obj->_base_offset);
            unless ( $tag ) {
                flock $fh, LOCK_UN;
                $self->_throw_error("Corrupted file, no master index record");
            }

            unless ($obj->_type eq $tag->{signature}) {
                flock $fh, LOCK_UN;
                $self->_throw_error("File type mismatch");
            }
        }
    }
    else {
        $self->calculate_sizes;
    }

    #XXX We have to make sure we don't mess up when autoflush isn't turned on
    $self->_storage->set_inode;

    flock $fh, LOCK_UN;

    return 1;
}

sub tag_size {
    my $self = shift;
    my ($size) = @_;
    return SIG_SIZE + $self->{data_size} + $size;
}

sub write_tag {
    ##
    # Given offset, signature and content, create tag and write to disk
    ##
    my $self = shift;
    my ($offset, $sig, $content) = @_;
    my $size = length( $content );

    $self->_storage->print_at(
        $offset, 
        $sig, pack($self->{data_pack}, $size), $content,
    );

    return unless defined $offset;

    return {
        signature => $sig,
        #XXX Is this even used?
        size      => $size,
        start     => $offset,
        offset    => $offset + SIG_SIZE + $self->{data_size},
        content   => $content,
        is_new    => 1,
    };
}

sub load_tag {
    ##
    # Given offset, load single tag and return signature, size and data
    ##
    my $self = shift;
    my ($offset) = @_;
    print join(":",map{$_||''}caller) . " - load_tag($offset)\n" if $::DEBUG;

    my $storage = $self->_storage;

    my ($sig, $size) = unpack(
        "A $self->{data_pack}",
        $storage->read_at( $offset, SIG_SIZE + $self->{data_size} ),
    );

    return {
        signature => $sig,
        size      => $size,   #XXX Is this even used?
        start     => $offset,
        offset    => $offset + SIG_SIZE + $self->{data_size},
        content   => $storage->read_at( undef, $size ),
    };
}

sub find_keyloc {
    my $self = shift;
    my ($tag, $transaction_id) = @_;
    $transaction_id = $self->_storage->transaction_id
        unless defined $transaction_id;

    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($loc, $trans_id, $is_deleted) = unpack(
            "$self->{long_pack} C C",
            substr( $tag->{content}, $i * $self->{key_size}, $self->{key_size} ),
        );

        next if $loc != HEAD && $transaction_id != $trans_id;
        return( $loc, $is_deleted, $i * $self->{key_size} );
    }

    return;
}

sub add_bucket {
    ##
    # Adds one key/value pair to bucket list, given offset, MD5 digest of key,
    # plain (undigested) key and value.
    ##
    my $self = shift;
    my ($tag, $md5, $plain_key, $value, $deleted, $orig_key) = @_;

    # This verifies that only supported values will be stored.
    {
        my $r = Scalar::Util::reftype( $value );

        last if !defined $r;
        last if $r eq 'HASH';
        last if $r eq 'ARRAY';

        $self->_throw_error(
            "Storage of references of type '$r' is not supported."
        );
    }

    my $storage = $self->_storage;

    #ACID - This is a mutation. Must only find the exact transaction
    my ($keyloc, $offset) = $self->_find_in_buckets( $tag, $md5, 1 );

    my @transactions;
    if ( $storage->transaction_id == 0 ) {
        @transactions = $storage->current_transactions;
    }

#    $self->_release_space( $size, $subloc );
#XXX This needs updating to use _release_space

    my $location;
    my $size = $self->_length_needed( $value, $plain_key );

    # Updating a known md5
    if ( $keyloc ) {
        my $keytag = $self->load_tag( $keyloc );
        my ($subloc, $is_deleted, $offset) = $self->find_keyloc( $keytag );

        if ( $subloc && !$is_deleted && @transactions ) {
            my $old_value = $self->read_from_loc( $subloc, $orig_key );
            my $old_size = $self->_length_needed( $old_value, $plain_key );

            for my $trans_id ( @transactions ) {
                my ($loc, $is_deleted, $offset2) = $self->find_keyloc( $keytag, $trans_id );
                unless ($loc) {
                    my $location2 = $storage->request_space( $old_size );
                    $storage->print_at( $keytag->{offset} + $offset2,
                        pack($self->{long_pack}, $location2 ),
                        pack( 'C C', $trans_id, 0 ),
                    );
                    $self->_write_value( $location2, $plain_key, $old_value, $orig_key );
                }
            }
        }

        $location = $self->_storage->request_space( $size );
        #XXX This needs to be transactionally-aware in terms of which keytag->{offset} to use
        $storage->print_at( $keytag->{offset} + $offset,
            pack($self->{long_pack}, $location ),
            pack( 'C C', $storage->transaction_id, 0 ),
        );
    }
    # Adding a new md5
    else {
        my $keyloc = $storage->request_space( $self->tag_size( $self->{keyloc_size} ) );

        # The bucket fit into list
        if ( defined $offset ) {
            $storage->print_at( $tag->{offset} + $offset,
                $md5, pack( $self->{long_pack}, $keyloc ),
            );
        }
        # If bucket didn't fit into list, split into a new index level
        else {
            $self->split_index( $tag, $md5, $keyloc );
        }

        my $keytag = $self->write_tag(
            $keyloc, SIG_KEYS, chr(0)x$self->{keyloc_size},
        );

        $location = $self->_storage->request_space( $size );
        $storage->print_at( $keytag->{offset},
            pack( $self->{long_pack}, $location ),
            pack( 'C C', $storage->transaction_id, 0 ),
        );

        my $offset = 1;
        for my $trans_id ( @transactions ) {
            $storage->print_at( $keytag->{offset} + $self->{key_size} * $offset++,
                pack( $self->{long_pack}, 0 ),
                pack( 'C C', $trans_id, 1 ),
            );
        }
    }

    $self->_write_value( $location, $plain_key, $value, $orig_key );

    return 1;
}

sub _write_value {
    my $self = shift;
    my ($key_loc, $location, $key, $value, $orig_key) = @_;

    my $storage = $self->_storage;

    my $dbm_deep_obj = _get_dbm_object( $value );
    if ( $dbm_deep_obj && $dbm_deep_obj->_storage ne $storage ) {
        $self->_throw_error( "Cannot cross-reference. Use export() instead" );
    }

    ##
    # Write signature based on content type, set content length and write
    # actual value.
    ##
    my $r = Scalar::Util::reftype( $value ) || '';
    if ( $dbm_deep_obj ) {
        $self->write_tag( $location, SIG_INTERNAL,pack($self->{long_pack}, $dbm_deep_obj->_base_offset) );
    }
    elsif ($r eq 'HASH') {
        if ( !$dbm_deep_obj && tied %{$value} ) {
            $self->_throw_error( "Cannot store something that is tied" );
        }
        $self->write_tag( $location, SIG_HASH, chr(0)x$self->{index_size} );
    }
    elsif ($r eq 'ARRAY') {
        if ( !$dbm_deep_obj && tied @{$value} ) {
            $self->_throw_error( "Cannot store something that is tied" );
        }
        $self->write_tag( $location, SIG_ARRAY, chr(0)x$self->{index_size} );
    }
    elsif (!defined($value)) {
        $self->write_tag( $location, SIG_NULL, '' );
    }
    else {
        $self->write_tag( $location, SIG_DATA, $value );
    }

    ##
    # Plain key is stored AFTER value, as keys are typically fetched less often.
    ##
    $storage->print_at( undef, pack($self->{data_pack}, length($key)) . $key );

    # Internal references don't care about autobless
    return 1 if $dbm_deep_obj;

    ##
    # If value is blessed, preserve class name
    ##
    if ( $storage->{autobless} ) {
        if ( defined( my $c = Scalar::Util::blessed($value) ) ) {
            $storage->print_at( undef, chr(1), pack($self->{data_pack}, length($c)) . $c );
        }
        else {
            $storage->print_at( undef, chr(0) );
        }
    }

    ##
    # Tie the passed in reference so that changes to it are reflected in the
    # datafile. The use of $location as the base_offset will act as the
    # the linkage between parent and child.
    #
    # The overall assignment is a hack around the fact that just tying doesn't
    # store the values. This may not be the wrong thing to do.
    ##
    if ($r eq 'HASH') {
        my %x = %$value;
        tie %$value, 'DBM::Deep', {
            base_offset => $key_loc,
            storage     => $storage,
            parent      => $self->{obj},
            parent_key  => $orig_key,
        };
        %$value = %x;
        bless $value, 'DBM::Deep::Hash' unless Scalar::Util::blessed( $value );
    }
    elsif ($r eq 'ARRAY') {
        my @x = @$value;
        tie @$value, 'DBM::Deep', {
            base_offset => $key_loc,
            storage     => $storage,
            parent      => $self->{obj},
            parent_key  => $orig_key,
        };
        @$value = @x;
        bless $value, 'DBM::Deep::Array' unless Scalar::Util::blessed( $value );
    }

    return 1;
}

sub split_index {
    my $self = shift;
    my ($tag, $md5, $keyloc) = @_;

    my $storage = $self->_storage;

    my $loc = $storage->request_space(
        $self->tag_size( $self->{index_size} ),
    );

    $storage->print_at( $tag->{ref_loc}, pack($self->{long_pack}, $loc) );

    my $index_tag = $self->write_tag(
        $loc, SIG_INDEX,
        chr(0)x$self->{index_size},
    );

    my $keys = $tag->{content}
             . $md5 . pack($self->{long_pack}, $keyloc);

    my @newloc = ();
    BUCKET:
    # The <= here is deliberate - we have max_buckets+1 keys to iterate
    # through, unlike every other loop that uses max_buckets as a stop.
    for (my $i = 0; $i <= $self->{max_buckets}; $i++) {
        my ($key, $old_subloc) = $self->_get_key_subloc( $keys, $i );

        die "[INTERNAL ERROR]: No key in split_index()\n" unless $key;
        die "[INTERNAL ERROR]: No subloc in split_index()\n" unless $old_subloc;

        my $num = ord(substr($key, $tag->{ch} + 1, 1));

        if ($newloc[$num]) {
            my $subkeys = $storage->read_at( $newloc[$num], $self->{bucket_list_size} );

            # This is looking for the first empty spot
            my ($subloc, $offset) = $self->_find_in_buckets(
                { content => $subkeys }, '',
            );

            $storage->print_at(
                $newloc[$num] + $offset,
                $key, pack($self->{long_pack}, $old_subloc),
            );

            next;
        }

        my $loc = $storage->request_space(
            $self->tag_size( $self->{bucket_list_size} ),
        );

        $storage->print_at(
            $index_tag->{offset} + ($num * $self->{long_size}),
            pack($self->{long_pack}, $loc),
        );

        my $blist_tag = $self->write_tag(
            $loc, SIG_BLIST,
            chr(0)x$self->{bucket_list_size},
        );

        $storage->print_at( $blist_tag->{offset}, $key . pack($self->{long_pack}, $old_subloc) );

        $newloc[$num] = $blist_tag->{offset};
    }

    $self->_release_space(
        $self->tag_size( $self->{bucket_list_size} ),
        $tag->{start},
    );

    return 1;
}

sub read_from_loc {
    my $self = shift;
    my ($key_loc, $subloc, $orig_key) = @_;

    my $storage = $self->_storage;

    my $signature = $storage->read_at( $subloc, SIG_SIZE );

    ##
    # If value is a hash or array, return new DBM::Deep object with correct offset
    ##
    if (($signature eq SIG_HASH) || ($signature eq SIG_ARRAY)) {
        #XXX This needs to be a singleton
#        my $new_obj;
#        my $is_autobless;
#        if ( $signature eq SIG_HASH ) {
#            $new_obj = {};
#            tie %$new_obj, 'DBM::Deep', {
#                base_offset => $subloc,
#                storage     => $self->_storage,
#                parent      => $self->{obj},
#                parent_key  => $orig_key,
#            };
#            $is_autobless = tied(%$new_obj)->_storage->{autobless};
#        }
#        else {
#            $new_obj = [];
#            tie @$new_obj, 'DBM::Deep', {
#                base_offset => $subloc,
#                storage     => $self->_storage,
#                parent      => $self->{obj},
#                parent_key  => $orig_key,
#            };
#            $is_autobless = tied(@$new_obj)->_storage->{autobless};
#        }
#
#        if ($is_autobless) {

        my $new_obj = DBM::Deep->new({
            type        => $signature,
            base_offset => $key_loc,
            storage     => $self->_storage,
            parent      => $self->{obj},
            parent_key  => $orig_key,
        });

        if ($new_obj->_storage->{autobless}) {
            ##
            # Skip over value and plain key to see if object needs
            # to be re-blessed
            ##
            $storage->increment_pointer( $self->{data_size} + $self->{index_size} );

            my $size = $storage->read_at( undef, $self->{data_size} );
            $size = unpack($self->{data_pack}, $size);
            if ($size) { $storage->increment_pointer( $size ); }

            my $bless_bit = $storage->read_at( undef, 1 );
            if ( ord($bless_bit) ) {
                my $size = unpack(
                    $self->{data_pack},
                    $storage->read_at( undef, $self->{data_size} ),
                );

                if ( $size ) {
                    $new_obj = bless $new_obj, $storage->read_at( undef, $size );
                }
            }
        }

        return $new_obj;
    }
    elsif ( $signature eq SIG_INTERNAL ) {
        my $size = $storage->read_at( undef, $self->{data_size} );
        $size = unpack($self->{data_pack}, $size);

        if ( $size ) {
            my $new_loc = $storage->read_at( undef, $size );
            $new_loc = unpack( $self->{long_pack}, $new_loc ); 
            return $self->read_from_loc( $key_loc, $new_loc, $orig_key );
        }
        else {
            return;
        }
    }
    ##
    # Otherwise return actual value
    ##
    elsif ( $signature eq SIG_DATA ) {
        my $size = $storage->read_at( undef, $self->{data_size} );
        $size = unpack($self->{data_pack}, $size);

        my $value = $size ? $storage->read_at( undef, $size ) : '';
        return $value;
    }

    ##
    # Key exists, but content is null
    ##
    return;
}

sub get_bucket_value {
    ##
    # Fetch single value given tag and MD5 digested key.
    ##
    my $self = shift;
    my ($tag, $md5, $orig_key) = @_;

    #ACID - This is a read. Can find exact or HEAD
    my ($keyloc, $offset) = $self->_find_in_buckets( $tag, $md5 );

    if ( !$keyloc ) {
        #XXX Need to use real key
#        $self->add_bucket( $tag, $md5, $orig_key, undef, $orig_key );
#        return;
    }
#    elsif ( !$is_deleted ) {
    else {
        my $keytag = $self->load_tag( $keyloc );
        my ($subloc, $is_deleted) = $self->find_keyloc( $keytag );
        if (!$subloc && !$is_deleted) {
            ($subloc, $is_deleted) = $self->find_keyloc( $keytag, 0 );
        }
        if ( $subloc && !$is_deleted ) {
            return $self->read_from_loc( $subloc, $orig_key );
        }
    }

    return;
}

sub delete_bucket {
    ##
    # Delete single key/value pair given tag and MD5 digested key.
    ##
    my $self = shift;
    my ($tag, $md5, $orig_key) = @_;

    #ACID - Although this is a mutation, we must find any transaction.
    # This is because we need to mark something as deleted that is in the HEAD.
    my ($keyloc, $offset) = $self->_find_in_buckets( $tag, $md5 );

    return if !$keyloc;

    my $storage = $self->_storage;

    my @transactions;
    if ( $storage->transaction_id == 0 ) {
        @transactions = $storage->current_transactions;
    }

    if ( $storage->transaction_id == 0 ) {
        my $keytag = $self->load_tag( $keyloc );

        my ($subloc, $is_deleted, $offset) = $self->find_keyloc( $keytag );
        return if !$subloc || $is_deleted;

        my $value = $self->read_from_loc( $subloc, $orig_key );

        my $size = $self->_length_needed( $value, $orig_key );

        for my $trans_id ( @transactions ) {
            my ($loc, $is_deleted, $offset2) = $self->find_keyloc( $keytag, $trans_id );
            unless ($loc) {
                my $location2 = $storage->request_space( $size );
                $storage->print_at( $keytag->{offset} + $offset2,
                    pack($self->{long_pack}, $location2 ),
                    pack( 'C C', $trans_id, 0 ),
                );
                $self->_write_value( $location2, $orig_key, $value, $orig_key );
            }
        }

        $keytag = $self->load_tag( $keyloc );
        ($subloc, $is_deleted, $offset) = $self->find_keyloc( $keytag );
        $storage->print_at( $keytag->{offset} + $offset,
            substr( $keytag->{content}, $offset + $self->{key_size} ),
            chr(0) x $self->{key_size},
        );
    }
    else {
        my $keytag = $self->load_tag( $keyloc );

        my ($subloc, $is_deleted, $offset) = $self->find_keyloc( $keytag );

        $storage->print_at( $keytag->{offset} + $offset,
            pack($self->{long_pack}, 0 ),
            pack( 'C C', $storage->transaction_id, 1 ),
        );
    }

    return 1;
}

sub bucket_exists {
    ##
    # Check existence of single key given tag and MD5 digested key.
    ##
    my $self = shift;
    my ($tag, $md5) = @_;

    #ACID - This is a read. Can find exact or HEAD
    my ($keyloc) = $self->_find_in_buckets( $tag, $md5 );
    my $keytag = $self->load_tag( $keyloc );
    my ($subloc, $is_deleted, $offset) = $self->find_keyloc( $keytag );
    if ( !$subloc && !$is_deleted ) {
        ($subloc, $is_deleted, $offset) = $self->find_keyloc( $keytag, 0 );
    }
    return ($subloc && !$is_deleted) && 1;
}

sub find_blist {
    ##
    # Locate offset for bucket list, given digested key
    ##
    my $self = shift;
    my ($offset, $md5, $args) = @_;
    $args = {} unless $args;

    ##
    # Locate offset for bucket list using digest index system
    ##
    my $tag = $self->load_tag( $offset )
        or $self->_throw_error( "INTERNAL ERROR - Cannot find tag" );

    #XXX What happens when $ch >= $self->{hash_size} ??
    for (my $ch = 0; $tag->{signature} ne SIG_BLIST; $ch++) {
        my $num = ord substr($md5, $ch, 1);

        my $ref_loc = $tag->{offset} + ($num * $self->{long_size});
        $tag = $self->index_lookup( $tag, $num );

        if (!$tag) {
            return if !$args->{create};

            my $loc = $self->_storage->request_space(
                $self->tag_size( $self->{bucket_list_size} ),
            );

            $self->_storage->print_at( $ref_loc, pack($self->{long_pack}, $loc) );

            $tag = $self->write_tag(
                $loc, SIG_BLIST,
                chr(0)x$self->{bucket_list_size},
            );

            $tag->{ref_loc} = $ref_loc;
            $tag->{ch} = $ch;

            last;
        }

        $tag->{ch} = $ch;
        $tag->{ref_loc} = $ref_loc;
    }

    return $tag;
}

sub index_lookup {
    ##
    # Given index tag, lookup single entry in index and return .
    ##
    my $self = shift;
    my ($tag, $index) = @_;

    my $location = unpack(
        $self->{long_pack},
        substr(
            $tag->{content},
            $index * $self->{long_size},
            $self->{long_size},
        ),
    );

    if (!$location) { return; }

    return $self->load_tag( $location );
}

sub traverse_index {
    ##
    # Scan index and recursively step into deeper levels, looking for next key.
    ##
    my $self = shift;
    my ($xxxx, $offset, $ch, $force_return_next) = @_;

    my $tag = $self->load_tag( $offset );

    if ($tag->{signature} ne SIG_BLIST) {
        my $start = $xxxx->{return_next} ? 0 : ord(substr($xxxx->{prev_md5}, $ch, 1));

        for (my $idx = $start; $idx < $self->{hash_chars_used}; $idx++) {
            my $subloc = unpack(
                $self->{long_pack},
                substr(
                    $tag->{content},
                    $idx * $self->{long_size},
                    $self->{long_size},
                ),
            );

            if ($subloc) {
                my $result = $self->traverse_index(
                    $xxxx, $subloc, $ch + 1, $force_return_next,
                );

                if (defined $result) { return $result; }
            }
        } # index loop

        $xxxx->{return_next} = 1;
    }
    # This is the bucket list
    else {
        my $keys = $tag->{content};
        if ($force_return_next) { $xxxx->{return_next} = 1; }

        ##
        # Iterate through buckets, looking for a key match
        ##
        my $transaction_id = $self->_storage->transaction_id;
        for (my $i = 0; $i < $self->{max_buckets}; $i++) {
            my ($key, $keyloc) = $self->_get_key_subloc( $keys, $i );

            # End of bucket list -- return to outer loop
            if (!$keyloc) {
                $xxxx->{return_next} = 1;
                last;
            }
            # Located previous key -- return next one found
            elsif ($key eq $xxxx->{prev_md5}) {
                $xxxx->{return_next} = 1;
                next;
            }
            # Seek to bucket location and skip over signature
            elsif ($xxxx->{return_next}) {
                my $storage = $self->_storage;

                my $keytag = $self->load_tag( $keyloc );
                my ($subloc, $is_deleted) = $self->find_keyloc( $keytag );
                if ( $subloc == 0 && !$is_deleted ) {
                    ($subloc, $is_deleted) = $self->find_keyloc( $keytag, 0 );
                }
                next if $is_deleted;

                # Skip over value to get to plain key
                my $sig = $storage->read_at( $subloc, SIG_SIZE );

                my $size = $storage->read_at( undef, $self->{data_size} );
                $size = unpack($self->{data_pack}, $size);
                if ($size) { $storage->increment_pointer( $size ); }

                # Read in plain key and return as scalar
                $size = $storage->read_at( undef, $self->{data_size} );
                $size = unpack($self->{data_pack}, $size);

                my $plain_key;
                if ($size) { $plain_key = $storage->read_at( undef, $size); }
                return $plain_key;
            }
        }

        $xxxx->{return_next} = 1;
    }

    return;
}

# Utilities

sub _get_key_subloc {
    my $self = shift;
    my ($keys, $idx) = @_;

    return unpack(
        # This is 'a', not 'A'. Please read the pack() documentation for the
        # difference between the two and why it's important.
        "a$self->{hash_size} $self->{long_pack}",
        substr(
            $keys,
            ($idx * $self->{bucket_size}),
            $self->{bucket_size},
        ),
    );
}

sub _find_in_buckets {
    my $self = shift;
    my ($tag, $md5) = @_;

    BUCKET:
    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($key, $subloc) = $self->_get_key_subloc(
            $tag->{content}, $i,
        );

        next BUCKET if $subloc && $key ne $md5;
        return( $subloc, $i * $self->{bucket_size} );
    }

    return;
}

sub _release_space {
    my $self = shift;
    my ($size, $loc) = @_;

    my $next_loc = 0;

    $self->_storage->print_at( $loc,
        SIG_FREE, 
        pack($self->{long_pack}, $size ),
        pack($self->{long_pack}, $next_loc ),
    );

    return;
}

sub _throw_error {
    die "DBM::Deep: $_[1]\n";
}

sub _get_dbm_object {
    my $item = shift;

    my $obj = eval {
        local $SIG{__DIE__};
        if ($item->isa( 'DBM::Deep' )) {
            return $item;
        }
        return;
    };
    return $obj if $obj;

    my $r = Scalar::Util::reftype( $item ) || '';
    if ( $r eq 'HASH' ) {
        my $obj = eval {
            local $SIG{__DIE__};
            my $obj = tied(%$item);
            if ($obj->isa( 'DBM::Deep' )) {
                return $obj;
            }
            return;
        };
        return $obj if $obj;
    }
    elsif ( $r eq 'ARRAY' ) {
        my $obj = eval {
            local $SIG{__DIE__};
            my $obj = tied(@$item);
            if ($obj->isa( 'DBM::Deep' )) {
                return $obj;
            }
            return;
        };
        return $obj if $obj;
    }

    return;
}

sub _length_needed {
    my $self = shift;
    my ($value, $key) = @_;

    my $is_dbm_deep = eval {
        local $SIG{'__DIE__'};
        $value->isa( 'DBM::Deep' );
    };

    my $len = SIG_SIZE
            + $self->{data_size} # size for value
            + $self->{data_size} # size for key
            + length( $key );    # length of key

    if ( $is_dbm_deep && $value->_storage eq $self->_storage ) {
        # long_size is for the internal reference
        return $len + $self->{long_size};
    }

    if ( $self->_storage->{autobless} ) {
        # This is for the bit saying whether or not this thing is blessed.
        $len += 1;
    }

    my $r = Scalar::Util::reftype( $value ) || '';
    unless ( $r eq 'HASH' || $r eq 'ARRAY' ) {
        if ( defined $value ) {
            $len += length( $value );
        }
        return $len;
    }

    $len += $self->{index_size};

    # if autobless is enabled, must also take into consideration
    # the class name as it is stored after the key.
    if ( $self->_storage->{autobless} ) {
        my $c = Scalar::Util::blessed($value);
        if ( defined $c && !$is_dbm_deep ) {
            $len += $self->{data_size} + length($c);
        }
    }

    return $len;
}

1;
__END__
