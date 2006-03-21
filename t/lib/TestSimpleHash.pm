package TestSimpleHash;

use 5.6.0;

use strict;
use warnings;

use Test::More;
use Test::Exception;

use base 'TestBase';

sub assignment_direct : Test( no_plan ) {
    my $self = shift;

    my $db = $self->{db};
    while ( my ($k,$v) = each %{$self->{data}} ) {
        $db->{$k} = $v;

        ok( $db->exists( $k ) );
        ok( exists $db->{$k} );

        is( $db->get($k), $v );
        is( $db->fetch($k), $v );
        is( $db->{$k}, $v );
    }
}

1;
__END__
