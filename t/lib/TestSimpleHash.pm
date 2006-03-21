package TestSimpleHash;

use 5.6.0;

use strict;
use warnings;

use Test::More;
use Test::Exception;

use base 'TestBase';

sub A_assignment : Test( no_plan ) {
    my $self = shift;
    my $db = $self->{db};

    my $rotate = 0;

    while ( my ($k,$v) = each %{$self->{data}} ) {
        $rotate = ++$rotate % 3;

        if ( $rotate == 0 ) {
            $db->{$k} = $v;
        }
        elsif ( $rotate == 1 ) {
            $db->put( $k => $v );
        }
        else {
            $db->store( $k => $v );
        }

        ok( $db->exists( $k ) );
        ok( exists $db->{$k} );

        is( $db->{$k}, $v );
        is( $db->get($k), $v );
        is( $db->fetch($k), $v );
    }
}

sub B_check_keys : Test( 1 ) {
    my $self = shift;
    my $db = $self->{db};

    my @control = sort keys %{$self->{data}};
    my @test1 = sort keys %$db;
    is_deeply( \@test1, \@control );
}

sub C_each : Test( 1 ) {
    my $self = shift;
    my $db = $self->{db};

    my $temp = {};
    while ( my ($k,$v) = each %$db ) {
        $temp->{$k} = $v;
    }

    is_deeply( $temp, $self->{data} );
}

sub D_firstkey : Test( 1 ) {
    my $self = shift;
    my $db = $self->{db};

    my $temp = {};

    my $key = $db->first_key;
    while ( $key ) {
        $temp->{$key} = $db->get( $key );
        $key = $db->next_key( $key );
    }

    is_deeply( $temp, $self->{data} );
}

1;
__END__
