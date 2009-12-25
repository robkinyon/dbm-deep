package DBM::Deep::Engine::DBI;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base 'DBM::Deep::Engine';

use DBM::Deep::Sector::DBI ();
use DBM::Deep::Storage::DBI ();

sub sector_type { 'DBM::Deep::Sector::DBI' }
sub iterator_class { 'DBM::Deep::Iterator::DBI' }

sub new {
    my $class = shift;
    my ($args) = @_;

    $args->{storage} = DBM::Deep::Storage::DBI->new( $args )
        unless exists $args->{storage};

    my $self = bless {
        storage => undef,
    }, $class;

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }

    return $self;
}

sub setup {
    my $self = shift;
    my ($obj) = @_;

    # Default the id to 1. This means that we will be creating a row if there
    # isn't one. The assumption is that the row_id=1 cannot never be deleted. I
    # don't know if this is a good assumption.
    $obj->{base_offset} ||= 1;

    my ($rows) = $self->storage->read_from(
        refs => $obj->_base_offset,
        qw( ref_type ),
    );

    # We don't have a row yet.
    unless ( @$rows ) {
        $self->storage->write_to(
            refs => $obj->_base_offset,
            ref_type => $obj->_type,
        );
    }

    my $sector = DBM::Deep::Sector::DBI::Reference->new({
        engine => $self,
        offset => $obj->_base_offset,
    });
}

sub read_value {
    my $self = shift;
    my ($obj, $key) = @_;

    my $sector = $self->load_sector( $obj->_base_offset, 'refs' )
        or return;

#    if ( $sector->staleness != $obj->_staleness ) {
#        return;
#    }

#    my $key_md5 = $self->_apply_digest( $key );

    my $value_sector = $sector->get_data_for({
        key => $key,
#        key_md5    => $key_md5,
        allow_head => 1,
    });

    unless ( $value_sector ) {
        $value_sector = DBM::Deep::Sector::DBI::Scalar->new({
            engine => $self,
            data   => undef,
        });

        $sector->write_data({
#            key_md5 => $key_md5,
            key     => $key,
            value   => $value_sector,
        });
    }

    return $value_sector->data;
}

=pod
sub get_classname {
    my $self = shift;
    my ($obj) = @_;
}

sub make_reference {
    my $self = shift;
    my ($obj, $old_key, $new_key) = @_;
}
=cut

# exists returns '', not undefined.
sub key_exists {
    my $self = shift;
    my ($obj, $key) = @_;

    my $sector = $self->load_sector( $obj->_base_offset, 'refs' )
        or return '';

#    if ( $sector->staleness != $obj->_staleness ) {
#        return '';
#    }

    my $data = $sector->get_data_for({
#        key_md5    => $self->_apply_digest( $key ),
        key        => $key,
        allow_head => 1,
    });

    # exists() returns 1 or '' for true/false.
    return $data ? 1 : '';
}

sub delete_key {
    my $self = shift;
    my ($obj, $key) = @_;

    my $sector = $self->load_sector( $obj->_base_offset, 'refs' )
        or return '';

#    if ( $sector->staleness != $obj->_staleness ) {
#        return '';
#    }

    return $sector->delete_key({
#        key_md5    => $self->_apply_digest( $key ),
        key        => $key,
        allow_head => 0,
    });
}

sub write_value {
    my $self = shift;
    my ($obj, $key, $value) = @_;

    my $r = Scalar::Util::reftype( $value ) || '';
    {
        last if $r eq '';
        last if $r eq 'HASH';
        last if $r eq 'ARRAY';

        DBM::Deep->_throw_error(
            "Storage of references of type '$r' is not supported."
        );
    }

    # Load the reference entry
    # Determine if the row was deleted under us
    # 

    my $sector = $self->load_sector( $obj->_base_offset, 'refs' )
        or die "Cannot load sector at '@{[$obj->_base_offset]}'\n";;

    my ($type, $class);
    if ( $r eq 'ARRAY' || $r eq 'HASH' ) {
        my $tmpvar;
        if ( $r eq 'ARRAY' ) {
            $tmpvar = tied @$value;
        } elsif ( $r eq 'HASH' ) {
            $tmpvar = tied %$value;
        }

        if ( $tmpvar ) {
            my $is_dbm_deep = eval { local $SIG{'__DIE__'}; $tmpvar->isa( 'DBM::Deep' ); };

            unless ( $is_dbm_deep ) {
                DBM::Deep->_throw_error( "Cannot store something that is tied." );
            }

            unless ( $tmpvar->_engine->storage == $self->storage ) {
                DBM::Deep->_throw_error( "Cannot store values across DBM::Deep files. Please use export() instead." );
            }

            # Load $tmpvar's sector

            # First, verify if we're storing the same thing to this spot. If we
            # are, then this should be a no-op. -EJS, 2008-05-19
            
            # See whether or not we are storing ourselves to ourself.
            # Write the sector as data in this reference (keyed by $key)
            my $value_sector = $self->load_sector( $tmpvar->_base_offset );
            $sector->write_data({
                key     => $key,
                key_md5 => $self->_apply_digest( $key ),
                value   => $value_sector,
            });
            $value_sector->increment_refcount;

            return 1;
        }

        $type = substr( $r, 0, 1 );
        $class = 'DBM::Deep::Sector::DBI::Reference';
    }
    else {
        if ( tied($value) ) {
            DBM::Deep->_throw_error( "Cannot store something that is tied." );
        }

        $class = 'DBM::Deep::Sector::DBI::Scalar';
        $type  = 'S';
    }

    # Create this after loading the reference sector in case something bad
    # happens. This way, we won't allocate value sector(s) needlessly.
    my $value_sector = $class->new({
        engine => $self,
        data   => $value,
        type   => $type,
    });

    $sector->write_data({
        key     => $key,
#        key_md5 => $self->_apply_digest( $key ),
        value   => $value_sector,
    });

    # This code is to make sure we write all the values in the $value to the
    # disk and to make sure all changes to $value after the assignment are
    # reflected on disk. This may be counter-intuitive at first, but it is
    # correct dwimmery.
    #   NOTE - simply tying $value won't perform a STORE on each value. Hence,
    # the copy to a temp value.
    if ( $r eq 'ARRAY' ) {
        my @temp = @$value;
        tie @$value, 'DBM::Deep', {
            base_offset => $value_sector->offset,
#            staleness   => $value_sector->staleness,
            storage     => $self->storage,
            engine      => $self,
        };
        @$value = @temp;
        bless $value, 'DBM::Deep::Array' unless Scalar::Util::blessed( $value );
    }
    elsif ( $r eq 'HASH' ) {
        my %temp = %$value;
        tie %$value, 'DBM::Deep', {
            base_offset => $value_sector->offset,
#            staleness   => $value_sector->staleness,
            storage     => $self->storage,
            engine      => $self,
        };

        %$value = %temp;
        bless $value, 'DBM::Deep::Hash' unless Scalar::Util::blessed( $value );
    }

    return 1;
}

sub begin_work {
    my $self = shift;
    my ($obj) = @_;
}

sub rollback {
    my $self = shift;
    my ($obj) = @_;
}

sub commit {
    my $self = shift;
    my ($obj) = @_;
}


1;
__END__
