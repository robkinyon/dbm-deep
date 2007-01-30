use strict;
use Test::More tests => 40;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
    file => $filename,
    locking => 1,
    autoflush => 1,
);

for ( 1 .. 17 ) {
    $db->{ $_ } = $_;
    is( $db->{$_}, $_, "Addition of $_ is still $_" );
}

for ( 1 .. 17 ) {
    is( $db->{$_}, $_, "Verification of $_ is still $_" );
}

my @keys = keys %$db;
cmp_ok( scalar(@keys), '==', 17, "Right number of keys returned" );

ok( !exists $db->{does_not_exist}, "EXISTS works on large hashes for non-existent keys" );
is( $db->{does_not_exist}, undef, "autovivification works on large hashes" );
ok( exists $db->{does_not_exist}, "EXISTS works on large hashes for newly-existent keys" );
cmp_ok( scalar(keys %$db), '==', 18, "Number of keys after autovivify is correct" );

