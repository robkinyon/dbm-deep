package TestSimpleArray;

use 5.6.0;

use strict;
use warnings;

use Test::More;
use Test::Exception;

use base 'TestBase';

sub A_assignment : Test( 37 ) {
    my $self = shift;
    my $db = $self->{db};

    my @keys = 0 .. $#{$self->{data}};

    push @keys, $keys[0] while @keys < 5;

    cmp_ok( @$db, '==', 0 );

    foreach my $k ( @keys[0..4] ) {
        ok( !exists $db->[$k] );
        ok( !$db->exists( $k ) );
    }

    $db->[$keys[0]] = $self->{data}[$keys[1]];
    $db->push( $self->{data}[$keys[2]] );
    $db->put( $keys[2] => $self->{data}[$keys[3]] );
    $db->store( $keys[3] => $self->{data}[$keys[4]] );
    $db->unshift( $self->{data}[$keys[0]] );

    foreach my $k ( @keys[0..4] ) {
        ok( $db->exists( $k ) );
        ok( exists $db->[$k] );

        is( $db->[$k], $self->{data}[$k] );
        is( $db->get($k), $self->{data}[$k] );
        is( $db->fetch($k), $self->{data}[$k] );
    }

    if ( @keys > 5 ) {
        $db->[$_] = $self->{data}[$_] for @keys[5..$#keys];
    }

    cmp_ok( @$db, '==', @keys );
}

1;
__END__
