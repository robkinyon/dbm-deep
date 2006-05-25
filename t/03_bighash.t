##
# DBM::Deep Test
##
use strict;
use Test::More;

plan skip_all => "You must set \$ENV{LONG_TESTS} to run the long tests"
    unless $ENV{LONG_TESTS};

use Test::Deep;
use t::common qw( new_fh );

plan tests => 5;

use_ok( 'DBM::Deep' );

diag "This test can take up to a minute to run. Please be patient.";

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
	type => DBM::Deep->TYPE_HASH,
);

##
# put/get many keys
##
my $max_keys = 4000;

for ( 0 .. $max_keys ) {
    $db->put( "hello $_" => "there " . $_ * 2 );
}

my $count = -1;
for ( 0 .. $max_keys ) {
    $count = $_;
    unless ( $db->get( "hello $_" ) eq "there " . $_ * 2 ) {
        last;
    };
}
is( $count, $max_keys, "We read $count keys" );

my @keys = sort keys %$db;
cmp_ok( scalar(@keys), '==', $max_keys + 1, "Number of keys is correct" );
my @control =  sort map { "hello $_" } 0 .. $max_keys;
cmp_deeply( \@keys, \@control, "Correct keys are there" );

$db->clear;
cmp_ok( scalar(keys %$db), '==', 0, "Number of keys after clear() is correct" );
