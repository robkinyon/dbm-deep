##
# DBM::Deep Test
##
use strict;
use Test::More tests => 5;
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

use_ok( 'DBM::Deep' );

my $dir = tempdir( CLEANUP => 1 );
my ($fh2, $filename2) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
flock $fh2, LOCK_UN;
my $db2 = DBM::Deep->new( $filename2 );

{
    my ($fh, $filename) = tempfile( 'tmpXXXX', UNLINK => 1, DIR => $dir );
    flock $fh, LOCK_UN;
    my $db = DBM::Deep->new( $filename );

    ##
    # Create structure in $db
    ##
    $db->import(
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
        }
    );
    is( $db->{hash1}{subkey1}, 'subvalue1', "Value imported correctly" );
    is( $db->{hash1}{subkey2}, 'subvalue2', "Value imported correctly" );

    ##
    # Cross-ref nested hash accross DB objects
    ##
    $db2->{copy} = $db->{hash1};
}

##
# Make sure $db2 has copy of $db's hash structure
##
is( $db2->{copy}{subkey1}, 'subvalue1', "Value copied correctly" );
is( $db2->{copy}{subkey2}, 'subvalue2', "Value copied correctly" );
