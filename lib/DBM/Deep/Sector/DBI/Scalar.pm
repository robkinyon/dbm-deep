package DBM::Deep::Sector::DBI::Scalar;

use strict;
use warnings FATAL => 'all';

use base qw( DBM::Deep::Sector::DBI );

sub table { 'datas' }

sub _init {
    my $self = shift;

    my $engine = $self->engine;
    unless ( $self->offset ) {
#        my ($rows) = $self->engine->storage->write_to(
#            datas => undef,
#            ( map { $_ => $self->{$_} } qw( ref_id data_type key value class ) ),
#        );
    }
    else {
        my ($rows) = $self->engine->storage->read_from(
            datas => $self->offset,
            qw( data_type key value class ),
        );

        $self->{$_} = $rows->[0]{$_} for qw( data_type key value class );
    }

    return;
}

sub data {
    my $self = shift;
    $self->{value};
}

=pod
sub write_data {
    my $self = shift;
    my ($args) = @_;

    $self->engine->storage->write_to(
        datas => $args->{value}{offset},
        ref_id    => $self->offset,
        data_type => 'S',
        key       => $args->{key},
        value     => $args->{value}{value},
        class     => $args->{value}{class},
    );

    $args->{value}->reload;
}
=cut

1;
__END__
