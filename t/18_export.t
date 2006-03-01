##
# DBM::Deep Test
##
use strict;
use Test::More tests => 2;
use File::Temp qw( tempfile tempdir );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );

my $struct;
{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    my $db = DBM::Deep->new( $filename );

    ##
    # Create structure in DB
    ##
    $db->import(
        key1 => "value1",
        key2 => "value2",
        array1 => [ "elem0", "elem1", "elem2", { foo => 'bar' }, [ 5 ] ],
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
        }
    );

    ##
    # Export entire thing
    ##
    $struct = $db->export();
}

##
# Make sure everything is here, outside DB
##
ok(
	($struct->{key1} eq "value1") && 
	($struct->{key2} eq "value2") && 
	($struct->{array1} && 
		($struct->{array1}->[0] eq "elem0") &&
		($struct->{array1}->[1] eq "elem1") && 
		($struct->{array1}->[2] eq "elem2") &&
		($struct->{array1}->[3]{foo} eq "bar") &&
		($struct->{array1}->[4][0] eq "5")
	) && 
	($struct->{hash1} &&
		($struct->{hash1}->{subkey1} eq "subvalue1") && 
		($struct->{hash1}->{subkey2} eq "subvalue2")
	)
);
