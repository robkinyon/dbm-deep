package DBM::Deep::Engine::Sector;

use 5.006_000;

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
sub type   { $_[0]{type}   }

sub base_size {
    my $self = shift;
    if ( ref($self) ) {
        return $self->engine->SIG_SIZE + $DBM::Deep::Engine::STALE_SIZE;
    }
    else {
        return $_[0]->SIG_SIZE + $DBM::Deep::Engine::STALE_SIZE;
    }
}

sub free {
    my $self = shift;

    my $e = $self->engine;

    $self->write( 0, $e->SIG_FREE );
    $self->write( $self->base_size, chr(0) x ($self->size - $self->base_size) );

    my $free_meth = $self->free_meth;
    $e->$free_meth( $self );

    return;
}

sub read {
    my $self = shift;

    if ( @_ == 1 ) {
        return substr( ${$self->engine->get_data( $self->offset, $self->size )}, $_[0] );
    }
    elsif ( @_ == 2 ) {
        return substr( ${$self->engine->get_data( $self->offset, $self->size )}, $_[0], $_[1] );
    }
    elsif ( @_ < 1 ) {
        die "read( start [, length ]): No parameters found.";
    }
    else {
        die "read( start [, length ]): Too many parameters found (@_).";
    }
}

sub write {
    my $self = shift;
    my ($start, $text) = @_;

    substr( ${$self->engine->get_data( $self->offset, $self->size )}, $start, length($text) ) = $text;

    $self->mark_dirty;
}

sub mark_dirty {
    my $self = shift;
    $self->engine->add_dirty_sector( $self->offset );
}

1;
__END__
