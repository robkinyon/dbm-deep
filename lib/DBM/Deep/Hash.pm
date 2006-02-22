package DBM::Deep::Hash;

use strict;

use base 'DBM::Deep';

sub _get_self {
    eval { tied( %{$_[0]} ) } || $_[0]
}

sub TIEHASH {
    ##
    # Tied hash constructor method, called by Perl's tie() function.
    ##
    my $class = shift;
    my $args;
    if (scalar(@_) > 1) {
        if ( @_ % 2 ) {
            $class->_throw_error( "Odd number of parameters to TIEHASH" );
        }
        $args = {@_};
    }
	elsif ( my $type = Scalar::Util::reftype($_[0]) ) {
        if ( $type ne 'HASH' ) {
            $class->_throw_error( "Not a hashref in TIEHASH" );
        }
        $args = $_[0];
    }
    else { $args = { file => shift }; }
    
    $args->{type} = $class->TYPE_HASH;

    return $class->_init($args);
}

sub FETCH {
    my $self = shift->_get_self;
    my $key = ($self->root->{filter_store_key})
        ? $self->root->{filter_store_key}->($_[0])
        : $_[0];

    return $self->SUPER::FETCH( $key );
}

sub STORE {
    my $self = shift->_get_self;
	my $key = ($self->root->{filter_store_key})
        ? $self->root->{filter_store_key}->($_[0])
        : $_[0];
    my $value = $_[1];

    return $self->SUPER::STORE( $key, $value );
}

sub EXISTS {
    my $self = shift->_get_self;
	my $key = ($self->root->{filter_store_key})
        ? $self->root->{filter_store_key}->($_[0])
        : $_[0];

    return $self->SUPER::EXISTS( $key );
}

sub FIRSTKEY {
	##
	# Locate and return first key (in no particular order)
	##
    my $self = $_[0]->_get_self;

	##
	# Make sure file is open
	##
	if (!defined($self->fh)) { $self->_open(); }
	
	##
	# Request shared lock for reading
	##
	$self->lock( $self->LOCK_SH );
	
	my $result = $self->_get_next_key();
	
	$self->unlock();
	
	return ($result && $self->root->{filter_fetch_key})
        ? $self->root->{filter_fetch_key}->($result)
        : $result;
}

sub NEXTKEY {
	##
	# Return next key (in no particular order), given previous one
	##
    my $self = $_[0]->_get_self;

	my $prev_key = ($self->root->{filter_store_key})
        ? $self->root->{filter_store_key}->($_[1])
        : $_[1];

	my $prev_md5 = $DBM::Deep::DIGEST_FUNC->($prev_key);

	##
	# Make sure file is open
	##
	if (!defined($self->fh)) { $self->_open(); }
	
	##
	# Request shared lock for reading
	##
	$self->lock( $self->LOCK_SH );
	
	my $result = $self->_get_next_key( $prev_md5 );
	
	$self->unlock();
	
	return ($result && $self->root->{filter_fetch_key})
        ? $self->root->{filter_fetch_key}->($result)
        : $result;
}

##
# Public method aliases
##
*first_key = *FIRSTKEY;
*next_key = *NEXTKEY;

1;
__END__
