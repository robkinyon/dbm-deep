##
# DBM::Deep Test
##
use strict;
use Test::More;

plan skip_all => "You must set \$ENV{LONG_TESTS} to run the long tests"
    unless $ENV{LONG_TESTS};

use Test::Deep;
use t::common qw( new_fh );

plan tests => 9;

use_ok( 'DBM::Deep' );

diag "This test can take up to a minute to run. Please be patient.";

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
	type => DBM::Deep->TYPE_HASH,
);

$db->{foo} = {};
my $foo = $db->{foo};

##
# put/get many keys
##
my $max_keys = 4000;

warn localtime(time) . ": before put\n";
for ( 0 .. $max_keys ) {
    $foo->put( "hello $_" => "there " . $_ * 2 );
}
warn localtime(time) . ": after put\n";

my $count = -1;
for ( 0 .. $max_keys ) {
    $count = $_;
    unless ( $foo->get( "hello $_" ) eq "there " . $_ * 2 ) {
        last;
    };
}
is( $count, $max_keys, "We read $count keys" );
warn localtime(time) . ": after read\n";

my @keys = sort keys %$foo;
warn localtime(time) . ": after keys\n";
cmp_ok( scalar(@keys), '==', $max_keys + 1, "Number of keys is correct" );
my @control =  sort map { "hello $_" } 0 .. $max_keys;
cmp_deeply( \@keys, \@control, "Correct keys are there" );

ok( !exists $foo->{does_not_exist}, "EXISTS works on large hashes for non-existent keys" );
is( $foo->{does_not_exist}, undef, "autovivification works on large hashes" );
ok( exists $foo->{does_not_exist}, "EXISTS works on large hashes for newly-existent keys" );
cmp_ok( scalar(keys %$foo), '==', $max_keys + 2, "Number of keys after autovivify is correct" );

warn localtime(time) . ": before clear\n";
$db->clear;
warn localtime(time) . ": after clear\n";
cmp_ok( scalar(keys %$db), '==', 0, "Number of keys after clear() is correct" );
