##
# DBM::Deep Test
##
use strict;
use Test::More tests => 3;

use_ok( 'DBM::Deep' );

##
# basic file open
##
unlink "t/test.db";
my $db = eval { DBM::Deep->new( "t/test.db" ) };
if ( DBM::Deep::error( $db ) || !$db ) {
	diag "ERROR: " . (DBM::Deep::error($db) || $@ || "UNKNOWN\n");
    Test::More->builder->BAIL_OUT( "Opening a new file fails" );
}

isa_ok( $db, 'DBM::Deep' );
ok(1, "We can successfully open a file!" );
