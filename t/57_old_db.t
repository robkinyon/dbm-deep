use strict;
use warnings FATAL => 'all';

use File::Spec::Functions 'catfile';
use Test::More;
use t::common qw( new_fh );

use DBM::Deep;

tie my %db, "DBM::Deep", catfile(< t etc db-1-0003 >);

is join("-", keys %db), "foo", '1.0003 db has one key';
is "@{$db{foo}}", "1 2 3", 'values in 1.0003 db';

done_testing;
