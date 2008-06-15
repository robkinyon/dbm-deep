package DBM::Deep::Engine::Sector;

use 5.006;

use strict;
use warnings FATAL => 'all';

use Scalar::Util ();

sub new {
    my $self = bless $_[1], $_[0];
    Scalar::Util::weaken( $self->{engine} );
    $self->_init;
    return $self;
}

#sub _init {}
#sub clone { DBM::Deep->_throw_error( "Must be implemented in the child class" ); }

sub engine { $_[0]{engine} }
sub offset { $_[0]{offset} }
sub type   { $_[0]{type} }

sub base_size {
   my $self = shift;
   no warnings 'once';
   return $self->engine->SIG_SIZE + $DBM::Deep::Engine::STALE_SIZE;
}

sub free {
    my $self = shift;

    my $e = $self->engine;

    $e->storage->print_at( $self->offset, $e->SIG_FREE );
    # Skip staleness counter
    $e->storage->print_at( $self->offset + $self->base_size,
        chr(0) x ($self->size - $self->base_size),
    );

    my $free_meth = $self->free_meth;
    $e->$free_meth( $self->offset, $self->size );

    return;
}

1;
__END__
