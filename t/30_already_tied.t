##
# DBM::Deep Test
##
use strict;
use Test::More tests => 7;
use Test::Exception;
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
flock $fh, LOCK_UN;
my $db = DBM::Deep->new( $filename );

{
    {
        package My::Tie::Hash;

        sub TIEHASH {
            my $class = shift;

            return bless {
            }, $class;
        }
    }

    my %hash;
    tie %hash, 'My::Tie::Hash';
    isa_ok( tied(%hash), 'My::Tie::Hash' );

    throws_ok {
        $db->{foo} = \%hash;
    } qr/Cannot store something that is tied/, "Cannot store tied hashes";
}

{
    {
        package My::Tie::Array;

        sub TIEARRAY {
            my $class = shift;

            return bless {
            }, $class;
        }

        sub FETCHSIZE { 0 }
    }

    my @array;
    tie @array, 'My::Tie::Array';
    isa_ok( tied(@array), 'My::Tie::Array' );

    throws_ok {
        $db->{foo} = \@array;
    } qr/Cannot store something that is tied/, "Cannot store tied arrays";
}

    {
        package My::Tie::Scalar;

        sub TIESCALAR {
            my $class = shift;

            return bless {
            }, $class;
        }
    }

    my $scalar;
    tie $scalar, 'My::Tie::Scalar';
    isa_ok( tied($scalar), 'My::Tie::Scalar' );

TODO: {
    local $TODO = "Scalar refs are just broked";
    throws_ok {
        $db->{foo} = \$scalar;
    } qr/Cannot store something that is tied/, "Cannot store tied scalars";
}
