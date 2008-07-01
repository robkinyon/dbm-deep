package DBM::Deep::Engine::Sector::Reference;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use Scalar::Util ();

use DBM::Deep::Null;

use DBM::Deep::Engine::Sector::Data;
our @ISA = qw( DBM::Deep::Engine::Sector::Data );

sub _init {
    my $self = shift;

    my $e = $self->engine;

    unless ( $self->offset ) {
        $self->{offset} = $e->_request_data_sector( $self->size );

        my $class_offset = 0;
        my $classname = Scalar::Util::blessed( delete $self->{data} );
        if ( defined $classname ) {
            my $class_sector = DBM::Deep::Engine::Sector::Scalar->new({
                engine => $e,
                data   => $classname,
            });
            $class_offset = $class_sector->offset;
        }

        $self->write( 0, $self->type );
        $self->write( $self->base_size, 
            pack( $e->StP($e->byte_size), 0 )             # Index/BList loc
          . pack( $e->StP($e->byte_size), $class_offset ) # Classname loc
          . pack( $e->StP($e->byte_size), 1 )             # Initial refcount
        );
    }
    else {
        $self->{type} = $self->read( 0, $e->SIG_SIZE );
    }

    $self->{staleness} = unpack(
        $e->StP($DBM::Deep::Engine::STALE_SIZE),
        $self->read( $e->SIG_SIZE, $DBM::Deep::Engine::STALE_SIZE ),
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
    return unpack(
        $e->StP($e->byte_size),
        $self->read( $self->base_size, $e->byte_size ),
    );
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

        $self->write( $self->base_size, pack( $engine->StP($engine->byte_size), $blist->offset ) );

        return $blist;
    }

    my $sector = $engine->_load_sector( $blist_loc )
        or DBM::Deep->_throw_error( "1: Cannot read sector at $blist_loc in get_bucket_list()" );
    my $i = 0;
    my $last_sector = undef;
    while ( $sector->isa( 'DBM::Deep::Engine::Sector::Index' ) ) {
        $blist_loc = $sector->get_entry( ord( substr( $args->{key_md5}, $i++, 1 ) ) );
        $last_sector = $sector;
        if ( $blist_loc ) {
            $sector = $engine->_load_sector( $blist_loc )
                or DBM::Deep->_throw_error( "2: Cannot read sector at $blist_loc in get_bucket_list()" );
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
        my $old_idx = 0;
        foreach my $entry ( $sector->chopped_up ) {
            my ($spot, $md5) = @{$entry};
            my $idx = ord( substr( $md5, $i, 1 ) );

            # XXX This is inefficient
            my $blist = $blist_cache{$idx}
                ||= DBM::Deep::Engine::Sector::BucketList->new({
                    engine => $engine,
                });

            $new_index->set_entry( $idx => $blist->offset );

            #XXX q.v. the comments for this function.
            my $new_idx = $blist->write_at_next_open( $md5 );

            $engine->reindex_entry( ( $sector->offset, $old_idx ) => ( $blist->offset, $new_idx ) );

            $old_idx++;
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

#XXX Why is this code here? -RobK, 2008-06-15
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
            $self->write( $self->base_size, pack( $engine->StP($engine->byte_size), $new_index->offset ) );
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
        $e->StP($e->byte_size),
        $self->read(
            $self->base_size + 1 * $e->byte_size,
            $e->byte_size,
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
        $e->StP($e->byte_size),
        $self->read(
            $self->base_size + 2 * $e->byte_size,
            $e->byte_size,
        ),
    );
}

sub write_refcount {
    my $self = shift;
    my ($num) = @_;

    my $e = $self->engine;
    $self->write(
        $self->base_size + 2 * $e->byte_size,
        pack( $e->StP($e->byte_size), $num ),
    );
}


1;
__END__

