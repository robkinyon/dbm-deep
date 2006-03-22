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

sub E_delete : Test( 12 ) {
    my $self = shift;
    my $db = $self->{db};

    my @keys = keys %{$self->{data}};
    cmp_ok( scalar(keys %$db), '==', scalar(@keys) );

    my $key1 = $keys[0];
    ok( exists $db->{$key1} );
    is( $db->{$key1}, $self->{data}{$key1} );
    is( delete $db->{$key1}, $self->{data}{$key1} );
    ok( !exists $db->{$key1} );
    cmp_ok( keys %$db, '==', @keys - 1 );

    my $key2 = $keys[1];
    ok( exists $db->{$key2} );
    is( $db->{$key2}, $self->{data}{$key2} );
    is( $db->delete( $key2 ), $self->{data}{$key2} );
    ok( !exists $db->{$key2} );
    cmp_ok( keys %$db, '==', @keys - 2 );

    @{$db}{ @keys[0,1] } = @{$self->{data}}{@keys[0,1]};

    cmp_ok( scalar(keys %$db), '==', scalar(@keys) );
}

sub F_clear : Test( 3 ) {
    my $self = shift;
    my $db = $self->{db};

    my @keys = keys %{$self->{data}};
    cmp_ok( scalar(keys %$db), '==', scalar(@keys) );

    %$db = ();

    cmp_ok( keys %$db, '==', 0 );

    %$db = %{$self->{data}};
    cmp_ok( scalar(keys %$db), '==', scalar(@keys) );
}

sub G_reassign_and_close : Test( 4 ) {
    my $self = shift;

    my @keys = keys %{$self->{data}};

    my $key1 = $keys[0];

    my $long_value = 'long value' x 100;
    $self->{db}{$key1} = $long_value;
    is( $self->{db}{$key1}, $long_value );

    my $filename = $self->{db}->_root->{file};
    undef $self->{db};

    $self->{db} = DBM::Deep->new( $filename );

    is( $self->{db}{$key1}, $long_value );

    $self->{db}{$key1} = $self->{data}{$key1};
    is( $self->{db}{$key1}, $self->{data}{$key1} );

    cmp_ok( keys %{$self->{db}}, '==', @keys );
}

1;
__END__
