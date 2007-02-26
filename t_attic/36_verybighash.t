# This test was contributed by Fedor Soreks, Jan 2007.

use strict;
use Test::More;

plan skip_all => "You must set \$ENV{LONG_TESTS} >= 2 to run the superlong tests"
    unless $ENV{LONG_TESTS} && $ENV{LONG_TESTS} >= 2;

use Test::Deep;
use t::common qw( new_fh );

plan tests => 2;

use_ok( 'DBM::Deep' );

diag "This test can take up to several hours to run. Please be VERY patient.";

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
    type => DBM::Deep->TYPE_HASH,
);

my $gigs = 2;

##
# put/get many keys
##
my $max_keys = 4_000_000;
my $max_record_keys = 10;

for my $key_no ( 0 .. $max_keys ) {
    for my $rec_no ( 0 .. $max_record_keys ) {
        $db->{"key_$key_no"}{"rec_key_$rec_no"} = "rec_val_$rec_no";
    }

    my $s = -s $filename;
    print "$key_no: $s\n";

    if ( $s > $gigs * 2**30) {
        fail "DB file ($filename) size exceeds $gigs GB";
        exit;
    }
}

ok( 1, "We passed the test!" );
