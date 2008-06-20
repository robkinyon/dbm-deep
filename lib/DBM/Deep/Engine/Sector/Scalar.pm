#TODO: Add chaining back in.
package DBM::Deep::Engine::Sector::Scalar;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

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
        my $next_offset = 0;

        if ( $dlen > $data_section ) {
            DBM::Deep->_throw_error( "Storage of values longer than $data_section not supported." );
        }

        $self->write( 0, $self->type );
        $self->write( $self->base_size,
            pack( $engine->StP($engine->byte_size), $next_offset ) # Chain loc
          . pack( $engine->StP(1), $dlen )                         # Data length
          . $data
        );

        return;
    }
}

sub data_length {
    my $self = shift;

    return unpack(
        $self->engine->StP(1),
        $self->read( $self->base_size + $self->engine->byte_size, 1 ),
    );
}

sub chain_loc {
    my $self = shift;
    return unpack(
        $self->engine->StP($self->engine->byte_size),
        $self->read(
            $self->base_size,
            $self->engine->byte_size,
        ),
    );
}

sub data {
    my $self = shift;

    my $data;
    while ( 1 ) {
        my $chain_loc = $self->chain_loc;

        $data .= $self->read( $self->base_size + $self->engine->byte_size + 1, $self->data_length );

        last unless $chain_loc;

        $self = $self->engine->_load_sector( $chain_loc );
    }

    return $data;
}

1;
__END__
