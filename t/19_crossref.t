##
# DBM::Deep Test
##
use strict;
use Test::More tests => 9;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh2, $filename2) = new_fh();
my $db2 = DBM::Deep->new( file => $filename2, fh => $fh2, );

SKIP: {
    skip "Apparently, we cannot detect a tied scalar?", 1;
    tie my $foo, 'Tied::Scalar';
    throws_ok {
        $db2->{failure} = $foo;
    } qr/Cannot store something that is tied\./, "tied scalar storage fails";
}

{
    tie my @foo, 'Tied::Array';
    throws_ok {
        $db2->{failure} = \@foo;
    } qr/Cannot store something that is tied\./, "tied array storage fails";
}

{
    tie my %foo, 'Tied::Hash';
    throws_ok {
        $db2->{failure} = \%foo;
    } qr/Cannot store something that is tied\./, "tied hash storage fails";
}

{
    my ($fh, $filename) = new_fh();
    my $db = DBM::Deep->new( file => $filename, fh => $fh, );

    ##
    # Create structure in $db
    ##
    $db->import({
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
        }
    });
    is( $db->{hash1}{subkey1}, 'subvalue1', "Value imported correctly" );
    is( $db->{hash1}{subkey2}, 'subvalue2', "Value imported correctly" );

    # Test cross-ref nested hash accross DB objects
    throws_ok {
        $db2->{copy} = $db->{hash1};
    } qr/Cannot store values across DBM::Deep files\. Please use export\(\) instead\./, "cross-ref fails";

    # This error text is for when internal cross-refs are implemented
    #} qr/Cannot cross-reference\. Use export\(\) instead\./, "cross-ref fails";

    $db2->{copy} = $db->{hash1}->export;
}

##
# Make sure $db2 has copy of $db's hash structure
##
is( $db2->{copy}{subkey1}, 'subvalue1', "Value copied correctly" );
is( $db2->{copy}{subkey2}, 'subvalue2', "Value copied correctly" );

package Tied::Scalar;
sub TIESCALAR { bless {}, $_[0]; }
sub FETCH{}

package Tied::Array;
sub TIEARRAY { bless {}, $_[0]; }

package Tied::Hash;
sub TIEHASH { bless {}, $_[0]; }
