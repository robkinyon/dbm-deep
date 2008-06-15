#TODO: Convert this to a string
package DBM::Deep::Engine::Sector::Scalar;

use 5.006;

use strict;
use warnings FATAL => 'all';

our $VERSION = '0.01';

use DBM::Deep::Engine::Sector::Data;
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
        $self->{offset} = $engine->_request_data_sector( $self->size );
        my $data = delete $self->{data};
        my $dlen = length $data;

        my $data_section = $self->size - $self->base_size - $engine->byte_size - 1;



        my $curr_offset = $self->offset;
        my $continue = 1;
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

            my $string = chr(0) x $self->size;
            substr( $string, 0, 1, $self->type );
            substr( $string, $self->base_size, $engine->byte_size + 1,
                pack( $engine->StP($engine->byte_size), $next_offset ) # Chain loc
              . pack( $engine->StP(1), $this_len ),                    # Data length
            );
            substr( $string, $self->base_size + $engine->byte_size + 1, $this_len,
                $chunk,
            );

            $engine->storage->print_at( $curr_offset, $string );

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

    return unpack( $self->engine->StP(1), $buffer );
}

sub chain_loc {
    my $self = shift;
    return unpack(
        $self->engine->StP($self->engine->byte_size),
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

1;
__END__
