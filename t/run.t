use 5.6.0;

use strict;
use warnings;

use lib 't/lib';

use DBM::Deep;

use Test1;

my $test = Test1->new(
    data => {
        key1 => 'value1',
        key2 => undef,
        key3 => 1.23,
    },
);

$test->runtests;
