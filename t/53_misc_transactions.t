
# This was discussed here:
# http://groups.google.com/group/DBM-Deep/browse_thread/thread/a6b8224ffec21bab
# brought up by Alex Gallichotte

use strict;
use Test;
use DBM::Deep;
use t::common qw( new_fh );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( file => $filename, fh => $fh, );

plan tests => 3;

eval { $db->{randkey()} = randkey() for 1 .. 10; }; ok($@, "");

eval {
    $db->begin_work;
    $db->{randkey()} = randkey() for 1 .. 10;
    $db->commit;
};
ok($@, '');

eval { $db->{randkey()} = randkey() for 1 .. 10; }; ok($@, "");

sub randkey {
    our $i ++;
    my @k = map { int rand 100 } 1 .. 10;
    local $" = "-";

    return "$i-@k";
}
