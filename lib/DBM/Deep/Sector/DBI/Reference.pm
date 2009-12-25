package DBM::Deep::Sector::DBI::Reference;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base 'DBM::Deep::Sector::DBI';

sub table { 'refs' }

sub _init {
    my $self = shift;

    my $e = $self->engine;

    unless ( $self->offset ) {
        $self->{offset} = $self->engine->storage->write_to(
            refs => undef,
            ref_type => $self->type,
        );
    }
    else {
        my ($rows) = $self->engine->storage->read_from(
            refs => $self->offset,
            qw( ref_type ),
        );

        $self->{type} = $rows->[0]{ref_type};
    }

    return;
}

sub get_data_for {
    my $self = shift;
    my ($args) = @_;

    my ($rows) = $self->engine->storage->read_from(
        datas => { ref_id => $self->offset, key => $args->{key} },
        qw( id ),
    );

    return unless $rows->[0]{id};

    $self->load(
        $self->engine,
        $rows->[0]{id},
        'datas',
    );
}

sub write_data {
    my $self = shift;
    my ($args) = @_;

    $self->engine->storage->write_to(
        datas => $args->{value}{offset},
        ref_id    => $self->offset,
        data_type => 'S',
        key       => $args->{key},
        value     => $args->{value}{data},
        class     => $args->{value}{class},
    );

    $args->{value}->reload;
}

sub delete_key {
    my $self = shift;
    my ($args) = @_;

    my $old_value = $self->get_data_for({
        key => $args->{key},
    });

    my $data;
    if ( $old_value ) {
        $data = $old_value->data;
        $old_value->free;
    }

    return $data;
}

1;
__END__
