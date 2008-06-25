package DBM::Deep::Engine::Sector::Index;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Engine::Sector;
our @ISA = qw( DBM::Deep::Engine::Sector );

sub _init {
    my $self = shift;

    my $engine = $self->engine;

    unless ( $self->offset ) {
        $self->{offset} = $engine->_request_index_sector( $self->size );

        $self->write( 0, $engine->SIG_INDEX );
    }

    return $self;
}

#XXX Change here
#XXX Why? -RobK, 2008-06-18
sub size {
    my $self = shift;
    if ( ref($self) ) {
        unless ( $self->{size} ) {
            my $e = $self->engine;
            $self->{size} = $self->base_size + $e->byte_size * $e->hash_chars;
        }
        return $self->{size};
    }
    else {
        my $e = shift;
        return $self->base_size($e) + $e->byte_size * $e->hash_chars;
    }
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
    return $self->base_size + $idx * $self->engine->byte_size;
}

sub get_entry {
    my $self = shift;
    my ($idx) = @_;

    my $e = $self->engine;

    DBM::Deep->_throw_error( "get_entry: Out of range ($idx)" )
        if $idx < 0 || $idx >= $e->hash_chars;

    return unpack(
        $e->StP($e->byte_size),
        $self->read( $self->_loc_for( $idx ), $e->byte_size ),
    );
}

sub set_entry {
    my $self = shift;
    my ($idx, $loc) = @_;

    my $e = $self->engine;

    DBM::Deep->_throw_error( "set_entry: Out of range ($idx)" )
        if $idx < 0 || $idx >= $e->hash_chars;

    $self->write( $self->_loc_for( $idx ), pack( $e->StP($e->byte_size), $loc ) );
}

1;
__END__
