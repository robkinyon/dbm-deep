package DBM::Deep::Iterator::DBI;

use strict;
use warnings FATAL => 'all';

use base qw( DBM::Deep::Iterator );

sub reset {
    my $self = shift;

    eval { $self->{sth}->finish; };
    delete $self->{sth};

    return;
}

sub get_next_key {
    my $self = shift;
    my ($obj) = @_;

    unless ( exists $self->{sth} ) {
        $self->{sth} = $self->{engine}->storage->{dbh}->prepare(
            "SELECT `key` FROM datas WHERE ref_id = ? ORDER BY RAND()",
        );
        $self->{sth}->execute( $self->{base_offset} );
    }

    my ($key) = $self->{sth}->fetchrow_array;
    return $key;
}

1;
__END__
