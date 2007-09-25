##
# DBM::Deep Test
##
use strict;
use Test::More tests => 10;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new(
	file => $filename,
);

$db->{foo} = 5;
$db->{bar} = $db->{foo};

is( $db->{foo}, 5, "Foo is still 5" );
is( $db->{bar}, 5, "Bar is now 5" );

$db->{foo} = 6;

is( $db->{foo}, 6, "Foo is now 6" );
is( $db->{bar}, 5, "Bar is still 5" );

$db->{foo} = [ 1 .. 3 ];
$db->{bar} = $db->{foo};

is( $db->{foo}[1], 2, "Foo[1] is still 2" );
is( $db->{bar}[1], 2, "Bar[1] is now 2" );

$db->{foo}[3] = 42;

is( $db->{foo}[3], 42, "Foo[3] is now 42" );
is( $db->{bar}[3], 42, "Bar[3] is also 42" );

delete $db->{foo};
is( $db->{bar}[3], 42, "After delete Foo, Bar[3] is still 42" );
