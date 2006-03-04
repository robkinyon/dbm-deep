package DBM::Deep::Scalar;

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

1;
__END__
