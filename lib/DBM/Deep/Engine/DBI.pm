package DBM::Deep::Engine::DBI;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base 'DBM::Deep::Engine';

use DBM::Deep::Sector::DBI ();
use DBM::Deep::Storage::DBI ();

sub sector_type { 'DBM::Deep::Sector::DBI' }

__END__

sub read_value {
    my $self = shift;
    my ($obj, $key) = @_;
}

sub get_classname {
    my $self = shift;
    my ($obj) = @_;
}

sub make_reference {
    my $self = shift;
    my ($obj, $old_key, $new_key) = @_;
}

sub key_exists {
    my $self = shift;
    my ($obj, $key) = @_;
}

sub delete_key {
    my $self = shift;
    my ($obj, $key) = @_;
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

    my ($type);
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
            $value_sector->increment_refcount;

            return 1;
        }

        $type = substr( $r, 0, 1 );
    }
    else {
        if ( tied($value) ) {
            DBM::Deep->_throw_error( "Cannot store something that is tied." );
        }
    }

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
            staleness   => $value_sector->staleness,
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
            staleness   => $value_sector->staleness,
            storage     => $self->storage,
            engine      => $self,
        };

        %$value = %temp;
        bless $value, 'DBM::Deep::Hash' unless Scalar::Util::blessed( $value );
    }

    return 1;
}

sub setup {
    my $self = shift;
    my ($obj) = @_;
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
