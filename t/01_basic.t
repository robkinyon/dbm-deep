##
# DBM::Deep Test
##
use strict;
use Test::More tests => 3;

diag "Testing DBM::Deep against Perl $] located at $^X";

use_ok( 'DBM::Deep' );

##
# basic file open
##
unlink "t/test.db";
my $db = eval { DBM::Deep->new( "t/test.db" ) };
if ( $@ ) {
	diag "ERROR: $@";
    Test::More->builder->BAIL_OUT( "Opening a new file fails" );
}

isa_ok( $db, 'DBM::Deep' );
ok(1, "We can successfully open a file!" );
