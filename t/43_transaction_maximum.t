use strict;
use Test::More;
use Test::Deep;
use Test::Exception;
use t::common qw( new_fh );

use DBM::Deep;

my $max_txns = 250;

my ($fh, $filename) = new_fh();

my @dbs = grep { $_ } map {
    my $x = 
    eval {
        DBM::Deep->new(
            file     => $filename,
            num_txns => $max_txns,
        );
    };
    die $@ if $@;
    $x;
} 1 .. $max_txns;

my $num = $#dbs;

plan tests => do {
    my $n = $num + 1;
    2 * $n;
};

my %trans_ids;
for my $n (0 .. $num) {
    lives_ok {
        $dbs[$n]->begin_work
    } "DB $n can begin_work";

    my $trans_id = $dbs[$n]->_engine->trans_id;
    ok( !exists $trans_ids{ $trans_id }, "DB $n has a unique transaction ID ($trans_id)" );
    $trans_ids{ $trans_id } = $n;
}
