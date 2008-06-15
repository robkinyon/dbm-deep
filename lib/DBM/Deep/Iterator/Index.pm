package DBM::Deep::Iterator::Index;

use 5.006;

use strict;
use warnings FATAL => 'all';

sub new {
    my $self = bless $_[1] => $_[0];
    $self->{curr_index} = 0;
    return $self;
}

sub at_end {
    my $self = shift;
    return $self->{curr_index} >= $self->{iterator}{engine}->hash_chars;
}

sub get_next_iterator {
    my $self = shift;

    my $loc;
    while ( !$loc ) {
        return if $self->at_end;
        $loc = $self->{sector}->get_entry( $self->{curr_index}++ );
    }

    return $self->{iterator}->get_sector_iterator( $loc );
}

1;
__END__
