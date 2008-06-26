package DBM::Deep::Engine::Sector::FileHeader;

use 5.006;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Engine::Sector;
our @ISA = qw( DBM::Deep::Engine::Sector );

my $header_fixed = length( &DBM::Deep::Engine::SIG_FILE ) + 1 + 4 + 4;
my $this_file_version = 3;

sub _init {
    my $self = shift;

    my $e = $self->engine;

    # This means the file is being created.
    unless ( exists $self->engine->sector_cache->{0} || $self->engine->storage->size ) {
        my $nt = $e->num_txns;
        my $bl = $e->txn_bitfield_len;

        my $header_var = $self->header_var_size;

        $self->{offset} = $e->storage->request_space( $header_fixed + $header_var );
        DBM::Deep::_throw_error( "Offset wasn't 0, it's '$self->{offset}'" ) unless $self->offset == 0;

        # Make sure we set up sector caching so that get_data() works. -RobK, 2008-06-24
        $self->engine->sector_cache->{$self->offset} = chr(0) x ($header_fixed + $header_var);

        $self->write( 0,
            $e->SIG_FILE
          . $e->SIG_HEADER
          . pack('N', $this_file_version) # At this point, we're at 9 bytes
          . pack('N', $header_var)        # header size
            # --- Above is $header_fixed. Below is $header_var
          . pack('C', $e->byte_size)

            # These shenanigans are to allow a 256 within a C
          . pack('C', $e->max_buckets - 1)
          . pack('C', $e->data_sector_size - 1)

          . pack('C', $nt)
          . pack('C' . $bl, 0 )                           # Transaction activeness bitfield
          . pack($e->StP($DBM::Deep::Engine::STALE_SIZE).($nt-1), 0 x ($nt-1) ) # Transaction staleness counters
          . pack($e->StP($e->byte_size), 0) # Start of free chain (blist size)
          . pack($e->StP($e->byte_size), 0) # Start of free chain (data size)
          . pack($e->StP($e->byte_size), 0) # Start of free chain (index size)
        );

        $e->set_trans_loc( $header_fixed + 4 );
        $e->set_chains_loc( $header_fixed + 4 + $bl + $DBM::Deep::Engine::STALE_SIZE * ($nt-1) );

        $self->{is_new} = 1;
    }
    else {
        $self->{offset} = 0;
        $self->{is_new} = 0;

        return if exists $self->engine->sector_cache->{0};

        my $s = $e->storage;

        my $buffer = $s->read_at( $self->offset, $header_fixed );
        return unless length($buffer);

        my ($file_signature, $sig_header, $file_version, $size) = unpack(
            'A4 A N N', $buffer
        );

        unless ( $file_signature eq $e->SIG_FILE ) {
            $s->close;
            DBM::Deep->_throw_error( "Signature not found -- file is not a Deep DB" );
        }

        unless ( $sig_header eq $e->SIG_HEADER ) {
            $s->close;
            DBM::Deep->_throw_error( "Pre-1.00 file version found" );
        }

        unless ( $file_version == $this_file_version ) {
            $s->close;
            DBM::Deep->_throw_error(
                "Wrong file version found - " .  $file_version .
                " - expected " . $this_file_version
            );
        }

        my $buffer2 = $s->read_at( undef, $size );
        my @values = unpack( 'C C C C', $buffer2 );

        if ( @values != 4 || grep { !defined } @values ) {
            $s->close;
            DBM::Deep->_throw_error("Corrupted file - bad header");
        }

        #XXX Add warnings if values weren't set right
        @{$e}{qw(byte_size max_buckets data_sector_size num_txns)} = @values;

        # These shenangians are to allow a 256 within a C
        $e->{max_buckets} += 1;
        $e->{data_sector_size} += 1;

        my $header_var = $self->header_var_size;
        unless ( $size == $header_var ) {
            $s->close;
            DBM::Deep->_throw_error( "Unexpected size found ($size <-> $header_var)." );
        }

        $e->set_trans_loc( $header_fixed + scalar(@values) );

        my $bl = $e->txn_bitfield_len;
        $e->set_chains_loc( $header_fixed + scalar(@values) + $bl + $DBM::Deep::Engine::STALE_SIZE * ($e->num_txns - 1) );

        # Make sure we set up sector caching so that get_data() works. -RobK, 2008-06-24
        $self->engine->sector_cache->{$self->offset} = $buffer . $buffer2;
    }
}

sub header_var_size {
    my $self = shift;
    my $e = shift || $self->engine;
    return 1 + 1 + 1 + 1 + $e->txn_bitfield_len + $DBM::Deep::Engine::STALE_SIZE * ($e->num_txns - 1) + 3 * $e->byte_size;
}

sub size {
    my $self = shift;
    if ( ref($self) ) {
        $self->{size} ||= $header_fixed + $self->header_var_size;
    }
    else {
        return $header_fixed + $self->header_var_size( @_ );
    }
}

sub is_new { $_[0]{is_new} }

sub add_free_sector {
    my $self = shift;
    my ($multiple, $sector) = @_;

    my $e = $self->engine;

    my $chains_offset = $multiple * $e->byte_size;

    # Increment staleness.
    # XXX Can this increment+modulo be done by "&= 0x1" ?
    my $staleness = unpack( $e->StP($DBM::Deep::Engine::STALE_SIZE), $sector->read( $e->SIG_SIZE, $DBM::Deep::Engine::STALE_SIZE ) );
    $staleness = ($staleness + 1 ) % ( 2 ** ( 8 * $DBM::Deep::Engine::STALE_SIZE ) );
    $sector->write( $e->SIG_SIZE, pack( $e->StP($DBM::Deep::Engine::STALE_SIZE), $staleness ) );

    my $old_head = $self->read( $e->chains_loc + $chains_offset, $e->byte_size );

    $self->write( $e->chains_loc + $chains_offset,
        pack( $e->StP($e->byte_size), $sector->offset ),
    );

    # Record the old head in the new sector after the signature and staleness counter
    $sector->write( $e->SIG_SIZE + $DBM::Deep::Engine::STALE_SIZE, $old_head );
}

sub request_sector {
    my $self = shift;
    my ($multiple, $size) = @_;

    my $e = $self->engine;

    my $chains_offset = $multiple * $e->byte_size;

    my $old_head = $self->read( $e->chains_loc + $chains_offset, $e->byte_size );
    my $loc = unpack( $e->StP($e->byte_size), $old_head );

    # We don't have any free sectors of the right size, so allocate a new one.
    unless ( $loc ) {
        my $offset = $e->storage->request_space( $size );

        # Zero out the new sector. This also guarantees correct increases
        # in the filesize.
        $self->engine->sector_cache->{$offset} = chr(0) x $size;

        return $offset;
    }

    # Need to load the new sector so we can read from it.
    my $new_sector = $self->engine->get_data( $loc, $size );

    # Read the new head after the signature and the staleness counter
    my $new_head = substr( $$new_sector, $e->SIG_SIZE + $DBM::Deep::Engine::STALE_SIZE, $e->byte_size );

    $self->write( $e->chains_loc + $chains_offset, $new_head );

    return $loc;
}

1;
__END__
