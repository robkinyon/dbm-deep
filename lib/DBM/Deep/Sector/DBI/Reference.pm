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

    if ( ( $args->{value}->type || 'S' ) eq 'S' ) {
        $self->engine->storage->write_to(
            datas => $args->{value}{offset},
            ref_id    => $self->offset,
            data_type => 'S',
            key       => $args->{key},
            value     => $args->{value}{data},
            class     => $args->{value}{class},
        );
    }
    else {
        $self->engine->storage->write_to(
            datas => $args->{value}{offset},
            ref_id    => $self->offset,
            data_type => 'R',
            key       => $args->{key},
            value     => $args->{value}{offset},
            class     => $args->{value}{class},
        );
    }

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
        $data = $old_value->data({ export => 1 });
        $old_value->free;
    }

    return $data;
}

sub get_classname {
    my $self = shift;
    return;
}

sub data {
    my $self = shift;
    my ($args) = @_;
    $args ||= {};

    my $obj = DBM::Deep->new({
        type        => $self->type,
        base_offset => $self->offset,
#        staleness   => $self->staleness,
        storage     => $self->engine->storage,
        engine      => $self->engine,
    });

    if ( $self->engine->storage->{autobless} ) {
        my $classname = $self->get_classname;
        if ( defined $classname ) {
            bless $obj, $classname;
        }
    }

    # We're not exporting, so just return.
    unless ( $args->{export} ) {
        return $obj;
    }

    # We shouldn't export if this is still referred to.
    if ( $self->get_refcount > 1 ) {
        return $obj;
    }

    return $obj->export;
}

sub free {
    my $self = shift;

    # We're not ready to be removed yet.
    if ( $self->decrement_refcount > 0 ) {
        return;
    }

    $self->engine->storage->delete_from(
        'datas', { ref_id => $self->offset },
    );

    $self->engine->storage->delete_from(
        'datas', { value => $self->offset, data_type => 'R' },
    );

    $self->SUPER::free( @_ );
}

sub increment_refcount {
    return 1;
}

sub decrement_refcount {
    return 0;
}

sub get_refcount {
    return 1;
}

sub write_refcount {
    my $self = shift;
    my ($num) = @_;
}

1;
__END__
