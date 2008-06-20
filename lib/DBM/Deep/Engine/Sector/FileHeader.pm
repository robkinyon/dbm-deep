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
    # Use defined() here because the offset should always be 0. -RobK. 2008-06-20
    unless ( $e->storage->size ) {
        my $nt = $e->num_txns;
        my $bl = $e->txn_bitfield_len;

        my $header_var = $self->header_var_size;

        $self->{offset} = $e->storage->request_space( $header_fixed + $header_var );
        DBM::Deep::_throw_error( "Offset wasn't 0, it's '$self->{offset}'" ) unless $self->offset == 0;

        $self->write( $self->offset,
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

        $self->{is_new} = 1;
    }
}

sub header_var_size {
    my $self = shift;
    my $e = $self->engine;
    return 1 + 1 + 1 + 1 + $e->txn_bitfield_len + $DBM::Deep::Engine::STALE_SIZE * ($e->num_txns - 1) + 3 * $e->byte_size;
}

sub size   {
    my $self = shift;
    $self->{size} ||= $header_fixed + $self->header_var_size;
}
sub is_new { $_[0]{is_new} }

1;
__END__
