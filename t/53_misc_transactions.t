
# This was discussed here:
# http://groups.google.com/group/DBM-Deep/browse_thread/thread/a6b8224ffec21bab
# brought up by Alex Gallichotte

use strict;
use warnings FATAL => 'all';

use Test::More tests => 4;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( file => $filename, fh => $fh );

eval { $db->{randkey()} = randkey() for 1 .. 10; }; ok(!$@, "No eval failures");

eval {
#    $db->begin_work;
    $db->{randkey()} = randkey() for 1 .. 10;
#    $db->commit;
};
ok(!$@, "No eval failures from the transaction");

eval { $db->{randkey()} = randkey() for 1 .. 10; }; ok(!$@, "No eval failures");

sub randkey {
    our $i ++;
    my @k = map { int rand 100 } 1 .. 10;
    local $" = "-";

    return "$i-@k";
}
