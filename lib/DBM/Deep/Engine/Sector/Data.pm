package DBM::Deep::Engine::Sector::Data;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base qw( DBM::Deep::Engine::Sector );

# This is in bytes
sub size { $_[0]{engine}->data_sector_size }
sub free_meth { return '_add_free_data_sector' }

sub clone {
    my $self = shift;
    return ref($self)->new({
        engine => $self->engine,
        type   => $self->type,
        data   => $self->data,
    });
}

1;
__END__
