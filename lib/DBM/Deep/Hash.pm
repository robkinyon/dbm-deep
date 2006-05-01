package DBM::Deep::Hash;

use 5.6.0;

use strict;
use warnings;

our $VERSION = q(0.99_01);

use base 'DBM::Deep';

sub _get_self {
    eval { local $SIG{'__DIE__'}; tied( %{$_[0]} ) } || $_[0]
}

#XXX Need to add a check here for @_ % 2
sub _repr { shift;return { @_ } }

sub _import {
    my $self = shift;
    my ($struct) = @_;

    eval {
        local $SIG{'__DIE__'};
        foreach my $key (keys %$struct) {
            $self->put($key, $struct->{$key});
        }
    }; if ($@) {
        $self->_throw_error("Cannot import: type mismatch");
    }

    return 1;
}

sub TIEHASH {
    ##
    # Tied hash constructor method, called by Perl's tie() function.
    ##
    my $class = shift;
    my $args = $class->_get_args( @_ );
    
    $args->{type} = $class->TYPE_HASH;

    return $class->_init($args);
}

sub FETCH {
    my $self = shift->_get_self;
    my $key = ($self->_fileobj->{filter_store_key})
        ? $self->_fileobj->{filter_store_key}->($_[0])
        : $_[0];

    return $self->SUPER::FETCH( $key, $_[0] );
}

sub STORE {
    my $self = shift->_get_self;
	my $key = ($self->_fileobj->{filter_store_key})
        ? $self->_fileobj->{filter_store_key}->($_[0])
        : $_[0];
    my $value = $_[1];

    return $self->SUPER::STORE( $key, $value, $_[0] );
}

sub EXISTS {
    my $self = shift->_get_self;
	my $key = ($self->_fileobj->{filter_store_key})
        ? $self->_fileobj->{filter_store_key}->($_[0])
        : $_[0];

    return $self->SUPER::EXISTS( $key );
}

sub DELETE {
    my $self = shift->_get_self;
	my $key = ($self->_fileobj->{filter_store_key})
        ? $self->_fileobj->{filter_store_key}->($_[0])
        : $_[0];

    return $self->SUPER::DELETE( $key, $_[0] );
}

sub FIRSTKEY {
	##
	# Locate and return first key (in no particular order)
	##
    my $self = shift->_get_self;

	##
	# Request shared lock for reading
	##
	$self->lock( $self->LOCK_SH );
	
	my $result = $self->_engine->get_next_key($self);
	
	$self->unlock();
	
	return ($result && $self->_fileobj->{filter_fetch_key})
        ? $self->_fileobj->{filter_fetch_key}->($result)
        : $result;
}

sub NEXTKEY {
	##
	# Return next key (in no particular order), given previous one
	##
    my $self = shift->_get_self;

	my $prev_key = ($self->_fileobj->{filter_store_key})
        ? $self->_fileobj->{filter_store_key}->($_[0])
        : $_[0];

	my $prev_md5 = $self->_engine->{digest}->($prev_key);

	##
	# Request shared lock for reading
	##
	$self->lock( $self->LOCK_SH );
	
	my $result = $self->_engine->get_next_key( $self, $prev_md5 );
	
	$self->unlock();
	
	return ($result && $self->_fileobj->{filter_fetch_key})
        ? $self->_fileobj->{filter_fetch_key}->($result)
        : $result;
}

##
# Public method aliases
##
sub first_key { (shift)->FIRSTKEY(@_) }
sub next_key { (shift)->NEXTKEY(@_) }

sub _copy_node {
    my $self = shift;
    my ($db_temp) = @_;

    my $key = $self->first_key();
    while ($key) {
        my $value = $self->get($key);
        $self->_copy_value( \$db_temp->{$key}, $value );
        $key = $self->next_key($key);
    }

    return 1;
}

1;
__END__
