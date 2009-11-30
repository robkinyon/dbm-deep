package DBM::Deep::Engine;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Iterator ();

# File-wide notes:
# * Every method in here assumes that the storage has been appropriately
#   safeguarded. This can be anything from flock() to some sort of manual
#   mutex. But, it's the caller's responsability to make sure that this has
#   been done.

# Setup file and tag signatures.  These should never change.
sub SIG_FILE     () { 'DPDB' }
sub SIG_HEADER   () { 'h'    }
sub SIG_HASH     () { 'H'    }
sub SIG_ARRAY    () { 'A'    }
sub SIG_NULL     () { 'N'    }
sub SIG_DATA     () { 'D'    }
sub SIG_INDEX    () { 'I'    }
sub SIG_BLIST    () { 'B'    }
sub SIG_FREE     () { 'F'    }
sub SIG_SIZE     () {  1     }

=head2 get_next_key( $obj, $prev_key )

This takes an object that provides _base_offset() and an optional string
representing the prior key returned via a prior invocation of this method.

This method delegates to C<< DBM::Deep::Iterator->get_next_key() >>.

=cut

# XXX Add staleness here
sub get_next_key {
    my $self = shift;
    my ($obj, $prev_key) = @_;

    # XXX Need to add logic about resetting the iterator if any key in the reference has changed
    unless ( $prev_key ) {
        $obj->{iterator} = DBM::Deep::Iterator->new({
            base_offset => $obj->_base_offset,
            engine      => $self,
        });
    }

    return $obj->{iterator}->get_next_key( $obj );
}

1;
__END__
