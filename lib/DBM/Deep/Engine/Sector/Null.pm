package DBM::Deep::Engine::Sector::Null;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base qw( DBM::Deep::Engine::Sector::Data );

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

1;
__END__
