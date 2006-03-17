##
# DBM::Deep Test
##
use strict;
use Test::More tests => 3;
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

diag "Testing DBM::Deep against Perl $] located at $^X";

use_ok( 'DBM::Deep' );

##
# basic file open
##
my $dir = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile( 'tmpXXXX', DIR => $dir );
flock $fh, LOCK_UN;
my $db = eval {
    local $SIG{__DIE__};
    DBM::Deep->new( $filename );
};
if ( $@ ) {
	diag "ERROR: $@";
    Test::More->builder->BAIL_OUT( "Opening a new file fails" );
}

isa_ok( $db, 'DBM::Deep' );
ok(1, "We can successfully open a file!" );
