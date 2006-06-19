package DBM::Deep::Engine2;

use base 'DBM::Deep::Engine';

use 5.6.0;

use strict;
use warnings;

our $VERSION = q(0.99_03);

use Fcntl qw( :DEFAULT :flock );
use Scalar::Util ();

# File-wide notes:
# * Every method in here assumes that the _storage has been appropriately
#   safeguarded. This can be anything from flock() to some sort of manual
#   mutex. But, it's the caller's responsability to make sure that this has
#   been done.

# Setup file and tag signatures.  These should never change.
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

sub read_value {
    my $self = shift;
    my ($trans_id, $base_offset, $key) = @_;
    
    my ($_val_offset, $_is_del) = $self->_find_value_offset({
        offset     => $base_offset,
        trans_id   => $trans_id,
        allow_head => 1,
    });
    die "Attempt to use a deleted value" if $_is_del;
    die "Internal error!" if !$_val_offset;

    my ($key_tag) = $self->_find_key_offset({
        offset  => $_val_offset,
        key_md5 => $self->_apply_digest( $key ),
    });
    return if !$key_tag;

    my ($val_offset, $is_del) = $self->_find_value_offset({
        offset     => $key_tag->{start},
        trans_id   => $trans_id,
        allow_head => 1,
    });
    return if $is_del;
    die "Internal error!" if !$val_offset;

    return $self->_read_value({
        keyloc => $key_tag->{start},
        offset => $val_offset,
        key    => $key,
    });
}

sub key_exists {
    my $self = shift;
    my ($trans_id, $base_offset, $key) = @_;
    
    my ($_val_offset, $_is_del) = $self->_find_value_offset({
        offset     => $base_offset,
        trans_id   => $trans_id,
        allow_head => 1,
    });
    die "Attempt to use a deleted value" if $_is_del;
    die "Internal error!" if !$_val_offset;

    my ($key_tag) = $self->_find_key_offset({
        offset  => $_val_offset,
        key_md5 => $self->_apply_digest( $key ),
    });
    return '' if !$key_tag->{start};

    my ($val_offset, $is_del) = $self->_find_value_offset({
        offset     => $key_tag->{start},
        trans_id   => $trans_id,
        allow_head => 1,
    });
    die "Internal error!" if !$_val_offset;

    return '' if $is_del;

    return 1;
}

sub get_next_key {
    my $self = shift;
    my ($trans_id, $base_offset) = @_;

    my ($_val_offset, $_is_del) = $self->_find_value_offset({
        offset     => $base_offset,
        trans_id   => $trans_id,
        allow_head => 1,
    });
    die "Attempt to use a deleted value" if $_is_del;
    die "Internal error!" if !$_val_offset;

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

    local $::DEBUG = 1;
    print "get_next_key: $_val_offset\n" if $::DEBUG;
    return $self->traverse_index( $temp, $_val_offset, 0 );
}

sub delete_key {
    my $self = shift;
    my ($trans_id, $base_offset, $key) = @_;

    my ($_val_offset, $_is_del) = $self->_find_value_offset({
        offset     => $base_offset,
        trans_id   => $trans_id,
        allow_head => 1,
    });
    die "Attempt to use a deleted value" if $_is_del;
    die "Internal error!" if !$_val_offset;

    my ($key_tag, $bucket_tag) = $self->_find_key_offset({
        offset  => $_val_offset,
        key_md5 => $self->_apply_digest( $key ),
    });
    return if !$key_tag->{start};

    my $value = $self->read_value( $trans_id, $base_offset, $key );
    my $value = $self->read_value( $trans_id, $base_offset, $key );
    if ( $trans_id ) {
        $self->_mark_as_deleted({
            tag      => $key_tag,
            trans_id => $trans_id,
        });
    }
    else {
        if ( my @transactions = $self->_storage->current_transactions ) {
            foreach my $other_trans_id ( @transactions ) {
                next if $self->_has_keyloc_entry({
                    tag      => $key_tag,
                    trans_id => $other_trans_id,
                });
                $self->write_value( $other_trans_id, $base_offset, $key, $value );
            }
        }

        $self->_mark_as_deleted({
            tag      => $key_tag,
            trans_id => $trans_id,
        });
#        $self->_remove_key_offset({
#            offset  => $_val_offset,
#            key_md5 => $self->_apply_digest( $key ),
#        });
    }

    return $value;
}

sub write_value {
    my $self = shift;
    my ($trans_id, $base_offset, $key, $value) = @_;

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

    my ($_val_offset, $_is_del) = $self->_find_value_offset({
        offset     => $base_offset,
        trans_id   => $trans_id,
        allow_head => 1,
    });
    die "Attempt to use a deleted value" if $_is_del;
    die "Internal error!" if !$_val_offset;

    my ($key_tag, $bucket_tag) = $self->_find_key_offset({
        offset  => $_val_offset,
        key_md5 => $self->_apply_digest( $key ),
        create  => 1,
    });
    die "Cannot find/create new key offset!" if !$key_tag->{start};

    if ( $trans_id ) {
        if ( $key_tag->{is_new} ) {
            # Must mark the HEAD as deleted because it doesn't exist
            $self->_mark_as_deleted({
                tag      => $key_tag,
                trans_id => HEAD,
            });
        }
    }
    else {
        # If the HEAD isn't new, then we must take other transactions
        # into account. If it is, then there can be no other transactions.
        if ( !$key_tag->{is_new} ) {
            my $old_value = $self->read_value( $trans_id, $base_offset, $key );
            if ( my @transactions = $self->_storage->current_transactions ) {
                foreach my $other_trans_id ( @transactions ) {
                    next if $self->_has_keyloc_entry({
                        tag      => $key_tag,
                        trans_id => $other_trans_id,
                    });
                    $self->write_value( $other_trans_id, $base_offset, $key, $old_value );
                }
            }
        }
    }

    my $value_loc = $self->_storage->request_space( 
        $self->_length_needed( $value, $key ),
    );

    $self->_add_key_offset({
        tag      => $key_tag,
        trans_id => $trans_id,
        loc      => $value_loc,
    });

    $self->_write_value( $key_tag->{start}, $value_loc, $key, $value, $key );

    return 1;
}

sub _find_value_offset {
    my $self = shift;
    my ($args) = @_;

    my $key_tag = $self->load_tag( $args->{offset} );

    my @head;
    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($loc, $trans_id, $is_deleted) = unpack(
            "$self->{long_pack} C C",
            substr( $key_tag->{content}, $i * $self->{key_size}, $self->{key_size} ),
        );

        if ( $trans_id == HEAD ) {
            @head = ($loc, $is_deleted);
        }

        next if $loc && $args->{trans_id} != $trans_id;
        return( $loc, $is_deleted );
    }

    return @head if $args->{allow_head};
    return;
}

sub _find_key_offset {
    my $self = shift;
    my ($args) = @_;

    my $bucket_tag = $self->load_tag( $args->{offset} )
        or $self->_throw_error( "INTERNAL ERROR - Cannot find tag" );

    #XXX What happens when $ch >= $self->{hash_size} ??
    for (my $ch = 0; $bucket_tag->{signature} ne SIG_BLIST; $ch++) {
        my $num = ord substr($args->{key_md5}, $ch, 1);

        my $ref_loc = $bucket_tag->{offset} + ($num * $self->{long_size});
        $bucket_tag = $self->index_lookup( $bucket_tag, $num );

        if (!$bucket_tag) {
            return if !$args->{create};

            my $loc = $self->_storage->request_space(
                $self->tag_size( $self->{bucket_list_size} ),
            );

            $self->_storage->print_at( $ref_loc, pack($self->{long_pack}, $loc) );

            $bucket_tag = $self->write_tag(
                $loc, SIG_BLIST,
                chr(0)x$self->{bucket_list_size},
            );

            $bucket_tag->{ref_loc} = $ref_loc;
            $bucket_tag->{ch} = $ch;
            $bucket_tag->{is_new} = 1;

            last;
        }

        $bucket_tag->{ch} = $ch;
        $bucket_tag->{ref_loc} = $ref_loc;
    }

    # Need to create a new keytag, too
    if ( $bucket_tag->{is_new} ) {
        my $keytag_loc = $self->_storage->request_space(
            $self->tag_size( $self->{keyloc_size} ),
        );

        substr( $bucket_tag->{content}, 0, $self->{key_size} ) =
            $args->{key_md5} . pack( "$self->{long_pack}", $keytag_loc );

        $self->_storage->print_at( $bucket_tag->{offset}, $bucket_tag->{content} );

        my $key_tag = $self->write_tag(
            $keytag_loc, SIG_KEYS,
            chr(0)x$self->{keyloc_size},
        );

        return( $key_tag, $bucket_tag );
    }
    else {
        my ($key, $subloc, $index);
        BUCKET:
        for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
            ($key, $subloc) = $self->_get_key_subloc(
                $bucket_tag->{content}, $i,
            );

            next BUCKET if $subloc && $key ne $args->{key_md5};

            # Keep track of where we are, in case we need to create a new
            # entry.
            $index = $i;
            last;
        }

        # If we have a subloc to return or we don't want to create a new
        # entry, we need to return now.
        $args->{create} ||= 0;
        return ($self->load_tag( $subloc ), $bucket_tag) if $subloc || !$args->{create};

        my $keytag_loc = $self->_storage->request_space(
            $self->tag_size( $self->{keyloc_size} ),
        );

        # There's space left in this bucket
        if ( defined $index ) {
            substr( $bucket_tag->{content}, $index * $self->{key_size}, $self->{key_size} ) =
                $args->{key_md5} . pack( "$self->{long_pack}", $keytag_loc );

            $self->_storage->print_at( $bucket_tag->{offset}, $bucket_tag->{content} );
        }
        # We need to split the index
        else {
            $self->split_index( $bucket_tag, $args->{key_md5}, $keytag_loc );
        }

        my $key_tag = $self->write_tag(
            $keytag_loc, SIG_KEYS,
            chr(0)x$self->{keyloc_size},
        );

        return( $key_tag, $bucket_tag );
    }

    return;
}

sub _read_value {
    my $self = shift;
    my ($args) = @_;

    return $self->read_from_loc( $args->{keyloc}, $args->{offset}, $args->{key} );
}

sub _mark_as_deleted {
    my $self = shift;
    my ($args) = @_;

    my $is_changed;
    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($loc, $trans_id, $is_deleted) = unpack(
            "$self->{long_pack} C C",
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ),
        );

        last unless $loc || $is_deleted;

        if ( $trans_id == $args->{trans_id} ) {
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ) = pack(
                "$self->{long_pack} C C",
                $loc, $trans_id, 1,
            );
            $is_changed = 1;
            last;
        }
    }

    if ( $is_changed ) {
        $self->_storage->print_at(
            $args->{tag}{offset}, $args->{tag}{content},
        );
    }

    return 1;
}

sub _has_keyloc_entry {
    my $self = shift;
    my ($args) = @_;

    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($loc, $trans_id, $is_deleted) = unpack(
            "$self->{long_pack} C C",
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ),
        );

        return 1 if $trans_id == $args->{trans_id};
    }

    return;
}

sub _remove_key_offset {
    my $self = shift;
    my ($args) = @_;

    my $is_changed;
    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($loc, $trans_id, $is_deleted) = unpack(
            "$self->{long_pack} C C",
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ),
        );

        if ( $trans_id == $args->{trans_id} ) {
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ) = '';
            $args->{tag}{content} .= chr(0) x $self->{key_size};
            $is_changed = 1;
            redo;
        }
    }

    if ( $is_changed ) {
        $self->_storage->print_at(
            $args->{tag}{offset}, $args->{tag}{content},
        );
    }

    return 1;
}

sub _add_key_offset {
    my $self = shift;
    my ($args) = @_;

    my $is_changed;
    for ( my $i = 0; $i < $self->{max_buckets}; $i++ ) {
        my ($loc, $trans_id, $is_deleted) = unpack(
            "$self->{long_pack} C C",
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ),
        );

        if ( $trans_id == $args->{trans_id} || (!$loc && !$is_deleted) ) {
            substr( $args->{tag}{content}, $i * $self->{key_size}, $self->{key_size} ) = pack(
                "$self->{long_pack} C C",
                $args->{loc}, $args->{trans_id}, 0,
            );
            $is_changed = 1;
            last;
        }
    }

    if ( $is_changed ) {
        $self->_storage->print_at(
            $args->{tag}{offset}, $args->{tag}{content},
        );
    }
    else {
        die "Why didn't _add_key_offset() change something?!\n";
    }

    return 1;
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
                $self->tag_size( $self->{keyloc_size} ),
            );

            my $value_spot = $self->_storage->request_space(
                $self->tag_size( $self->{index_size} ),
            );

            $self->write_tag(
                $obj->{base_offset}, SIG_KEYS,
                pack( "$self->{long_pack} C C", $value_spot, HEAD, 0 ),
                chr(0) x ($self->{index_size} - $self->{key_size}),
            );

            $self->write_tag(
                $value_spot, $obj->_type,
                chr(0)x$self->{index_size},
            );

            # Flush the filehandle
            my $old_fh = select $fh;
            my $old_af = $|; $| = 1; $| = $old_af;
            select $old_fh;
        }
        else {
            $obj->{base_offset} = $bytes_read;

            my ($_val_offset, $_is_del) = $self->_find_value_offset({
                offset     => $obj->{base_offset},
                trans_id   => HEAD,
                allow_head => 1,
            });
            die "Attempt to use a deleted value" if $_is_del;
            die "Internal error!" if !$_val_offset;

            ##
            # Get our type from master index header
            ##
            my $tag = $self->load_tag($_val_offset);
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

1;
__END__
