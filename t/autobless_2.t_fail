use strict;
$|++;

{
    package Foo;

    sub export { 'export' };
    sub foo { 'foo' };
}

use Test::More no_plan => 1;

use_ok( 'DBM::Deep' );

unlink 't/test.db';
my $db = DBM::Deep->new(
    file     => "t/test.db",
    autobless => 0,
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

$db->{unblessed} = {};
$db->{unblessed}{a} = 1;
$db->{unblessed}{b} = [];
$db->{unblessed}{b}[0] = 1;
$db->{unblessed}{b}[1] = 2;
$db->{unblessed}{b}[2] = 3;

undef $db;

my $db2 = DBM::Deep->new(
    file     => 't/test.db',
    autobless => 1,
);
if ($db2->error()) {
	die "ERROR: " . $db2->error();
}

is( $db2->{unblessed}{a}, 1 );
is( $db2->{unblessed}{b}[0], 1 );
is( $db2->{unblessed}{b}[1], 2 );
is( $db2->{unblessed}{b}[2], 3 );

$db2->{unblessed}{a} = 2;

is( $db2->{unblessed}{a}, 2 );
is( $db2->{unblessed}{b}[0], 1 );
is( $db2->{unblessed}{b}[1], 2 );
is( $db2->{unblessed}{b}[2], 3 );

undef $db2;

my $db3 = DBM::Deep->new(
    file     => "t/test.db",
    autobless => 0,
);
if ($db3->error()) {
	die "ERROR: " . $db->error();
}

is( $db3->{unblessed}{a}, 2 );
is( $db3->{unblessed}{b}[0], 1 );
__END__
is( $db3->{unblessed}{b}[1], 2 );
is( $db3->{unblessed}{b}[2], 3 );
