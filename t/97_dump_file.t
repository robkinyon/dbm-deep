use strict;
use Test::More tests => 3;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
);

is( $db->_dump_file, <<"__END_DUMP__", "Dump of initial file correct" );
NumTxns: 1
Chains(B):
Chains(D):
Chains(I):
00000030: H  0064 REF: 1
__END_DUMP__

$db->{foo} = 'bar';

is( $db->_dump_file, <<"__END_DUMP__", "Dump of initial file correct" );
NumTxns: 1
Chains(B):
Chains(D):
Chains(I):
00000030: H  0064 REF: 1
00000094: D  0064 bar
00000158: B  0387
    00000545 00000094
00000545: D  0064 foo
__END_DUMP__

