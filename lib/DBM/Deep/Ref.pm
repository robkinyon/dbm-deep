package DBM::Deep::Ref;

use strict;

use base 'DBM::Deep';

sub _get_self {
    eval { local $SIG{'__DIE__'}; tied( ${$_[0]} ) } || $_[0]
}

sub TIESCALAR {
    ##
    # Tied hash constructor method, called by Perl's tie() function.
    ##
    my $class = shift;
    my $args = $class->_get_args( @_ );
    
    $args->{type} = $class->TYPE_SCALAR;

    return $class->_init($args);
}

sub FETCH {
    my $self = shift->_get_self;

    #my $value = $self->
}

sub STORE {
    my $self = shift->_get_self;
    my ($value) = @_;
}

1;
__END__
