package DBM::Deep::Engine::Sector::BucketList;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Engine::Sector;
our @ISA = qw( DBM::Deep::Engine::Sector );

sub _init {
    my $self = shift;

    my $engine = $self->engine;

    unless ( $self->offset ) {
        $self->{offset} = $engine->_request_blist_sector( $self->size );

        $self->write( 0, $engine->SIG_BLIST );
    }

    if ( $self->{key_md5} ) {
        $self->find_md5;
    }

    return $self;
}

sub clear {
    my $self = shift;

    # Zero-fill the data
    $self->write( $self->base_size, chr(0) x ($self->size - $self->base_size) );
}

sub size {
    my $self = shift;
    if ( ref($self) ) {
        unless ( $self->{size} ) {
            # Base + numbuckets * bucketsize
            $self->{size} = $self->base_size + $self->engine->max_buckets * $self->bucket_size;
        }
        return $self->{size};
    }
    else {
        my $e = shift;
        return $self->base_size($e) + $e->max_buckets * $self->bucket_size($e);
    }
}

sub free_meth { return '_add_free_blist_sector' }

sub free {
    my $self = shift;

    my $e = $self->engine;
    foreach my $bucket ( $self->chopped_up ) {
        my $rest = $bucket->[-1];

        # Delete the keysector
        my $l = unpack( $e->StP($e->byte_size), substr( $rest, $e->hash_size, $e->byte_size ) );
        my $s = $e->_load_sector( $l ); $s->free if $s;

        # Delete the HEAD sector
        $l = unpack( $e->StP($e->byte_size),
            substr( $rest,
                $e->hash_size + $e->byte_size,
                $e->byte_size,
            ),
        );
        $s = $e->_load_sector( $l ); $s->free if $s; 

        foreach my $txn ( 0 .. $e->num_txns - 2 ) {
            my $l = unpack( $e->StP($e->byte_size),
                substr( $rest,
                    $e->hash_size + 2 * $e->byte_size + $txn * ($e->byte_size + $DBM::Deep::Engine::STALE_SIZE),
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
    if ( ref($self) ) {
        unless ( $self->{bucket_size} ) {
            my $e = $self->engine;
            # Key + head (location) + transactions (location + staleness-counter)
            my $location_size = $e->byte_size + $e->byte_size + ($e->num_txns - 1) * ($e->byte_size + $DBM::Deep::Engine::STALE_SIZE);
            $self->{bucket_size} = $e->hash_size + $location_size;
        }
        return $self->{bucket_size};
    }
    else {
        my $e = shift;
        my $location_size = $e->byte_size + $e->byte_size + ($e->num_txns - 1) * ($e->byte_size + $DBM::Deep::Engine::STALE_SIZE);
        return $e->hash_size + $location_size;
    }
}

# XXX This is such a poor hack. I need to rethink this code.
sub chopped_up {
    my $self = shift;

    my $e = $self->engine;

    my @buckets;
    foreach my $idx ( 0 .. $e->max_buckets - 1 ) {
        my $spot = $self->base_size + $idx * $self->bucket_size;
        my $data = $self->read( $spot, $self->bucket_size );

        # _dump_file() will run into the blank_md5. Otherwise, we should never run into it.
        # -RobK, 2008-06-18
        last if substr( $data, 0, $e->hash_size ) eq $e->blank_md5;

        push @buckets, [ $spot, $data ];
    }

    return @buckets;
}

#XXX Call this append() instead? -RobK, 2008-06-30
sub write_at_next_open {
    my $self = shift;
    my ($entry) = @_;

    #XXX This is such a hack!
    $self->{_next_open} = 0 unless exists $self->{_next_open};

    my $spot = $self->base_size + $self->{_next_open}++ * $self->bucket_size;
    $self->write( $spot, $entry );

    return ($self->{_next_open} - 1);
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
        my $potential = $self->read(
            $self->base_size + $idx * $self->bucket_size, $e->hash_size,
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

    my $e = $self->engine;

    $args->{trans_id} = $e->trans_id unless exists $args->{trans_id};

    my $spot = $self->base_size + $self->{idx} * $self->bucket_size;
    $e->add_entry( $args->{trans_id}, $self->offset, $self->{idx} );

    unless ($self->{found}) {
        my $key_sector = DBM::Deep::Engine::Sector::Scalar->new({
            engine => $e,
            data   => $args->{key},
        });

        $self->write( $spot, $args->{key_md5} . pack( $e->StP($e->byte_size), $key_sector->offset ) );
    }

    my $loc = $spot + $e->hash_size + $e->byte_size;

    if ( $args->{trans_id} ) {
        $loc += $e->byte_size + ($args->{trans_id} - 1) * ( $e->byte_size + $DBM::Deep::Engine::STALE_SIZE );

        $self->write( $loc,
            pack( $e->StP($e->byte_size), $args->{value}->offset )
          . pack( $e->StP($DBM::Deep::Engine::STALE_SIZE), $e->get_txn_staleness_counter( $args->{trans_id} ) ),
        );
    }
    else {
        $self->write( $loc, pack( $e->StP($e->byte_size), $args->{value}->offset ) );
    }
}

sub mark_deleted {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    my $e = $self->engine;

    $args->{trans_id} = $e->trans_id unless exists $args->{trans_id};

    my $spot = $self->base_size + $self->{idx} * $self->bucket_size;
    $e->add_entry( $args->{trans_id}, $self->offset, $self->{idx} );

    my $loc = $spot
      + $e->hash_size
      + $e->byte_size;

    if ( $args->{trans_id} ) {
        $loc += $e->byte_size + ($args->{trans_id} - 1) * ( $e->byte_size + $DBM::Deep::Engine::STALE_SIZE );

        $self->write( $loc,
            pack( $e->StP($e->byte_size), 1 ) # 1 is the marker for deleted
          . pack( $e->StP($DBM::Deep::Engine::STALE_SIZE), $e->get_txn_staleness_counter( $args->{trans_id} ) ),
        );
    }
    else {
        # 1 is the marker for deleted
        $self->write( $loc, pack( $e->StP($e->byte_size), 1 ) );
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

    my $spot = $self->base_size + $self->{idx} * $self->bucket_size;

    # Shuffle everything down to cover the deleted bucket's spot.
    $self->write( $spot,
        $self->read(
            $spot + $self->bucket_size,
            $self->bucket_size * ( $engine->max_buckets - $self->{idx} - 1 ),
        )
      . chr(0) x $self->bucket_size,
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

    my $spot = $self->base_size
      + $args->{idx} * $self->bucket_size
      + $e->hash_size
      + $e->byte_size;

    if ( $args->{trans_id} ) {
        $spot += $e->byte_size + ($args->{trans_id} - 1) * ( $e->byte_size + $DBM::Deep::Engine::STALE_SIZE );
    }

    my $buffer = $self->read( $spot, $e->byte_size + $DBM::Deep::Engine::STALE_SIZE );
    my ($loc, $staleness) = unpack(
        $e->StP($e->byte_size) . ' ' . $e->StP($DBM::Deep::Engine::STALE_SIZE),
        $buffer,
    );

    # XXX Merge the two if-clauses below
    if ( $args->{trans_id} ) {
        # We have found an entry that is old, so get rid of it
        if ( $staleness != (my $s = $e->get_txn_staleness_counter( $args->{trans_id} ) ) ) {
            $e->storage->print_at(
                $spot,
                pack( $e->StP($e->byte_size) . ' ' . $e->StP($DBM::Deep::Engine::STALE_SIZE), (0) x 2 ), 
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
        DBM::Deep->_throw_error( "get_key_for(): Attempting to retrieve $idx beyond max_buckets" );
    }

    my $location = $self->read(
        $self->base_size + $idx * $self->bucket_size + $self->engine->hash_size,
        $self->engine->byte_size,
    );
    $location = unpack( $self->engine->StP($self->engine->byte_size), $location );
    DBM::Deep->_throw_error( "get_key_for: No location?" ) unless $location;

    return $self->engine->_load_sector( $location );
}

sub rollback {
    my $self = shift;
    my ($idx) = @_;
    my $e = $self->engine;
    my $trans_id = $e->trans_id;

#    warn "Rolling back $idx ($trans_id)\n";

    my $base = $self->base_size + ($idx * $self->bucket_size) + $e->hash_size + $e->byte_size;
    my $spot = $base + $e->byte_size + ($trans_id - 1) * ( $e->byte_size + $DBM::Deep::Engine::STALE_SIZE );

    my $trans_loc = $self->read( $spot, $e->byte_size );
    $trans_loc = unpack( $e->StP($e->byte_size), $trans_loc );
#    warn "$trans_loc\n";

    $self->write( $spot, pack( $e->StP($e->byte_size), 0 ) );

    if ( $trans_loc > 1 ) {
        $e->_load_sector( $trans_loc )->free;
    }

    return;
}

sub commit {
    my $self = shift;
    my ($idx) = @_;
    my $e = $self->engine;
    my $trans_id = $e->trans_id;

    my $base = $self->base_size + ($idx * $self->bucket_size) + $e->hash_size + $e->byte_size;

    my $head_loc = $self->read( $base, $e->byte_size );
    $head_loc = unpack( $e->StP($e->byte_size), $head_loc );

    my $spot = $base + $e->byte_size + ($trans_id - 1) * ( $e->byte_size + $DBM::Deep::Engine::STALE_SIZE );
    my $trans_loc = $self->read( $spot, $e->byte_size );

    $self->write( $base, $trans_loc );
    $self->write( $spot, pack( $e->StP($e->byte_size) . ' ' . $e->StP($DBM::Deep::Engine::STALE_SIZE), (0) x 2 ) );

    if ( $head_loc > 1 ) {
        $e->_load_sector( $head_loc )->free;
    }

    return;
}

1;
__END__
