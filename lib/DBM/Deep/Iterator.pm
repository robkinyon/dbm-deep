package DBM::Deep::Iterator;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use DBM::Deep::Iterator::BucketList ();
use DBM::Deep::Iterator::Index ();

=head1 NAME

DBM::Deep::Iterator

=head1 PURPOSE

This is an internal-use-only object for L<DBM::Deep/>. It is the iterator
for FIRSTKEY() and NEXTKEY().

=head1 OVERVIEW

This object 

=head1 METHODS

=head2 new(\%params)

The constructor takes a hashref of params. The hashref is assumed to have the
following elements:

=over 4

=item * engine (of type L<DBM::Deep::Engine/>

=item * base_offset (the base_offset of the invoking DBM::Deep object)

=back

=cut

sub new {
    my $class = shift;
    my ($args) = @_;

    my $self = bless {
        breadcrumbs => [],
        engine      => $args->{engine},
        base_offset => $args->{base_offset},
    }, $class;

    Scalar::Util::weaken( $self->{engine} );

    return $self;
}

=head2 reset()

This method takes no arguments.

It will reset the iterator so that it will start from the beginning again.

This method returns nothing.

=cut

sub reset { $_[0]{breadcrumbs} = [] }

=head2 get_sector_iterator( $loc )

This takes a location. It will load the sector for $loc, then instantiate the right
iteartor type for it.

This returns the sector iterator.

=cut

sub get_sector_iterator {
    my $self = shift;
    my ($loc) = @_;

    my $sector = $self->{engine}->_load_sector( $loc )
        or return;

    if ( $sector->isa( 'DBM::Deep::Engine::Sector::Index' ) ) {
        return DBM::Deep::Iterator::Index->new({
            iterator => $self,
            sector   => $sector,
        });
    }
    elsif ( $sector->isa( 'DBM::Deep::Engine::Sector::BucketList' ) ) {
        return DBM::Deep::Iterator::BucketList->new({
            iterator => $self,
            sector   => $sector,
        });
    }

    DBM::Deep->_throw_error( "get_sector_iterator(): Why did $loc make a $sector?" );
}

=head2 get_next_key( $obj )

=cut

sub get_next_key {
    my $self = shift;
    my ($obj) = @_;

    my $crumbs = $self->{breadcrumbs};
    my $e = $self->{engine};

    unless ( @$crumbs ) {
        # This will be a Reference sector
        my $sector = $e->_load_sector( $self->{base_offset} )
            # If no sector is found, thist must have been deleted from under us.
            or return;

        if ( $sector->staleness != $obj->_staleness ) {
            return;
        }

        my $loc = $sector->get_blist_loc
            or return;

        push @$crumbs, $self->get_sector_iterator( $loc );
    }

    FIND_NEXT_KEY: {
        # We're at the end.
        unless ( @$crumbs ) {
            $self->reset;
            return;
        }

        my $iterator = $crumbs->[-1];

        # This level is done.
        if ( $iterator->at_end ) {
            pop @$crumbs;
            redo FIND_NEXT_KEY;
        }

        if ( $iterator->isa( 'DBM::Deep::Iterator::Index' ) ) {
            # If we don't have any more, it will be caught at the
            # prior check.
            if ( my $next = $iterator->get_next_iterator ) {
                push @$crumbs, $next;
            }
            redo FIND_NEXT_KEY;
        }

        unless ( $iterator->isa( 'DBM::Deep::Iterator::BucketList' ) ) {
            DBM::Deep->_throw_error(
                "Should have a bucketlist iterator here - instead have $iterator"
            );
        }

        # At this point, we have a BucketList iterator
        my $key = $iterator->get_next_key;
        if ( defined $key ) {
            return $key;
        }
        #XXX else { $iterator->set_to_end() } ?

        # We hit the end of the bucketlist iterator, so redo
        redo FIND_NEXT_KEY;
    }

    DBM::Deep->_throw_error( "get_next_key(): How did we get here?" );
}

1;
__END__
