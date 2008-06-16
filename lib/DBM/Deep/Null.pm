# This was copied from MARCEL's Class::Null. However, I couldn't use it because
# I need an undef value, not an implementation of the Null Class pattern.
package DBM::Deep::Null;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use overload
    'bool'   => sub { undef },
    '""'     => sub { undef },
    '0+'     => sub { undef },
    fallback => 1,
    nomethod => 'AUTOLOAD';

sub AUTOLOAD { return; }

1;
__END__
