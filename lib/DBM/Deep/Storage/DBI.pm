package DBM::Deep::Storage::DBI;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base 'DBM::Deep::Storage';

sub is_writable {
    my $self = shift;
    return 1;
}

sub lock_exclusive {
    my $self = shift;
}

sub lock_shared {
    my $self = shift;
}

sub unlock {
    my $self = shift;
}

1;
__END__
