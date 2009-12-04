package DBM::Deep::Sector;

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

sub _init {}
sub clone { die "clone must be implemented in a child class" }

sub engine { $_[0]{engine} }
sub offset { $_[0]{offset} }
sub type   { $_[0]{type}   }

sub load { die "load must be implemented in a child class" }

1;
__END__
