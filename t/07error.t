##
# DBM::Deep Test
##
use strict;
use Test::More tests => 5;

use_ok( 'DBM::Deep' );

unlink "t/test.db";
my $db = DBM::Deep->new( "t/test.db" );
if ($db->error()) {
	die "ERROR: " . $db->error();
}

##
# cause an error
##
eval { $db->push("foo"); }; # ERROR -- array-only method
ok( $db->error() );

##
# make sure you can clear the error state
##
$db->clear_error();
ok( !$db->error() );
undef $db;

##
# test a corrupted file
##
open FH, '>t/test.db';
print FH 'DPDB';
close FH;
eval { $db = DBM::Deep->new( "t/test.db" ); };
ok( $@ );

##
# test a file type mismatch
##
unlink "t/test.db";
my %hash;
tie %hash, 'DBM::Deep', 't/test.db';
$hash{'foo'} = 'bar';
undef %hash;
my @array;
eval { tie @array, 'DBM::Deep', 't/test.db'; };
ok( $@ );
