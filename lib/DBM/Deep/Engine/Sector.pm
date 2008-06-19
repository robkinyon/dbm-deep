package DBM::Deep::Engine::Sector;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use Scalar::Util ();

sub new {
    my $self = bless $_[1], $_[0];
    Scalar::Util::weaken( $self->{engine} );

    if ( $self->offset ) {
        $self->{string} = $self->engine->storage->read_at(
            $self->offset, $self->size,
        );
    }
    else {
        $self->{string} = chr(0) x $self->size;
    }

    $self->_init;

    return $self;
}

#sub _init {}
#sub clone { DBM::Deep->_throw_error( "Must be implemented in the child class" ); }

sub engine { $_[0]{engine} }
sub offset { $_[0]{offset} }
sub type   { $_[0]{type}   }

sub base_size {
   my $self = shift;
   no warnings 'once';
   return $self->engine->SIG_SIZE + $DBM::Deep::Engine::STALE_SIZE;
}

sub free {
    my $self = shift;

    my $e = $self->engine;

    $self->write( 0, $e->SIG_FREE );
    $self->write( $self->base_size, chr(0) x ($self->size - $self->base_size) );

    $e->flush;

#    $e->storage->print_at( $self->offset, $e->SIG_FREE );
#    # Skip staleness counter
#    $e->storage->print_at( $self->offset + $self->base_size,
#        chr(0) x ($self->size - $self->base_size),
#    );

    #TODO When freeing two sectors, we cannot flush them right away! This means the following:
    # 1) The header has to understand about unflushed items.
    # 2) Loading a sector has to go through a cache to make sure we see what's already been loaded.
    # 3) The header should be cached.

    my $free_meth = $self->free_meth;
    $e->$free_meth( $self->offset, $self->size );

    return;
}

sub read {
    my $self = shift;
    my ($start, $length) = @_;
    if ( $length ) {
        return substr( $self->{string}, $start, $length );
    }
    else {
        return substr( $self->{string}, $start );
    }
}

sub write {
    my $self = shift;
    my ($start, $text) = @_;

    substr( $self->{string}, $start, length($text) ) = $text;

    $self->mark_dirty;
}

sub mark_dirty {
    my $self = shift;
    $self->engine->add_dirty_sector( $self );
}

sub flush {
    my $self = shift;
    $self->engine->storage->print_at( $self->offset, $self->{string} );
}

1;
__END__
