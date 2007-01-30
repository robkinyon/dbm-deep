##
# DBM::Deep Test
##
use strict;
use Test::More tests => 11;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new({
        file      => $filename,
        autobless => 1,
    });

##
# Create structure in memory
##
    my $struct = {
        key1 => "value1",
        key2 => "value2",
        array1 => [ "elem0", "elem1", "elem2" ],
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
            subkey3 => bless( {}, 'Foo' ),
        }
    };

    $db->import( $struct );

    cmp_deeply(
        $db,
        noclass({
            key1 => 'value1',
            key2 => 'value2',
            array1 => [ 'elem0', 'elem1', 'elem2', ],
            hash1 => {
                subkey1 => "subvalue1",
                subkey2 => "subvalue2",
                subkey3 => useclass( bless {}, 'Foo' ),
            },
        }),
        "Everything matches",
    );

    $struct->{foo} = 'bar';
    is( $struct->{foo}, 'bar', "\$struct has foo and it's 'bar'" );
    ok( !exists $db->{foo}, "\$db doesn't have the 'foo' key, so \$struct is not tied" );

    $struct->{hash1}->{foo} = 'bar';
    is( $struct->{hash1}->{foo}, 'bar', "\$struct->{hash1} has foo and it's 'bar'" );
    ok( !exists $db->{hash1}->{foo}, "\$db->{hash1} doesn't have the 'foo' key, so \$struct->{hash1} is not tied" );
}

{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new({
        file => $filename,
        type => DBM::Deep->TYPE_ARRAY,
    });

    my $struct = [
        1 .. 3,
        [ 2, 4, 6 ],
        bless( [], 'Bar' ),
        { foo => [ 2 .. 4 ] },
    ];

    $db->import( $struct );

    cmp_deeply(
        $db,
        noclass([
            1 .. 3,
            [ 2, 4, 6 ],
            useclass( bless( [], 'Bar' ) ),
            { foo => [ 2 .. 4 ] },
        ]),
        "Everything matches",
    );

    push @$struct, 'bar';
    is( $struct->[-1], 'bar', "\$struct has 'bar' at the end" );
    ok( $db->[-1], "\$db doesn't have the 'bar' value at the end, so \$struct is not tied" );
}

# Failure case to verify that rollback occurs
{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new({
        file      => $filename,
        autobless => 1,
    });

    $db->{foo} = 'bar';

    my $x;
    my $struct = {
        key1 => [
            2, \$x, 3, 
        ],
    };

    eval {
        $db->import( $struct );
    };
    like( $@, qr/Storage of references of type 'SCALAR' is not supported/, 'Error message correct' );

    cmp_deeply(
        $db,
        noclass({
            foo => 'bar',
        }),
        "Everything matches",
    );
}

__END__

Need to add tests for:
    - Failure case (have something tied or a glob or something like that)
    - Where we already have $db->{hash1} to make sure that it's not overwritten
