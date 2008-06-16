package DBM::Deep::Iterator::BucketList;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

sub new {
    my $self = bless $_[1] => $_[0];
    $self->{curr_index} = 0;
    return $self;
}

sub at_end {
    my $self = shift;
    return $self->{curr_index} >= $self->{iterator}{engine}->max_buckets;
}

sub get_next_key {
    my $self = shift;

    return if $self->at_end;

    my $idx = $self->{curr_index}++;

    my $data_loc = $self->{sector}->get_data_location_for({
        allow_head => 1,
        idx        => $idx,
    }) or return;

    #XXX Do we want to add corruption checks here?
    return $self->{sector}->get_key_for( $idx )->data;
}

1;
__END__
