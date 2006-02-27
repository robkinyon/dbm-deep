use strict;

{
    package Foo;

    sub export { 'export' };
    sub foo { 'foo' };
}

use Test::More tests => 39;

use_ok( 'DBM::Deep' );

unlink 't/test.db';
my $db = DBM::Deep->new(
    file     => "t/test.db",
    autobless => 1,
);
if ($db->error()) {
	die "ERROR: " . $db->error();
}

my $obj = bless {
    a => 1,
    b => [ 1 .. 3 ],
}, 'Foo';

$db->{blessed} = $obj;

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

my $obj2 = $db2->{blessed};
isa_ok( $obj2, 'Foo' );
can_ok( $obj2, 'export', 'foo' );
ok( !$obj2->can( 'STORE' ), "... but it cannot 'STORE'" );

is( $obj2->{a}, 1 );
is( $obj2->{b}[0], 1 );
is( $obj2->{b}[1], 2 );
is( $obj2->{b}[2], 3 );

is( $db2->{unblessed}{a}, 1 );
is( $db2->{unblessed}{b}[0], 1 );
is( $db2->{unblessed}{b}[1], 2 );
is( $db2->{unblessed}{b}[2], 3 );

$obj2->{c} = 'new';
is( $db2->{blessed}{c}, 'new' );

undef $db2;

$db2 = DBM::Deep->new(
    file     => 't/test.db',
    autobless => 1,
);
is( $db2->{blessed}{c}, 'new' );

{
    my $structure = $db2->export();
    
    my $obj2 = $structure->{blessed};
    isa_ok( $obj2, 'Foo' );
    can_ok( $obj2, 'export', 'foo' );
    ok( !$obj2->can( 'STORE' ), "... but it cannot 'STORE'" );

    is( $obj2->{a}, 1 );
    is( $obj2->{b}[0], 1 );
    is( $obj2->{b}[1], 2 );
    is( $obj2->{b}[2], 3 );

    is( $structure->{unblessed}{a}, 1 );
    is( $structure->{unblessed}{b}[0], 1 );
    is( $structure->{unblessed}{b}[1], 2 );
    is( $structure->{unblessed}{b}[2], 3 );
}

my $db3 = DBM::Deep->new(
    file     => 't/test.db',
);
if ($db3->error()) {
	die "ERROR: " . $db3->error();
}

my $obj3 = $db3->{blessed};
isa_ok( $obj3, 'DBM::Deep' );
can_ok( $obj3, 'export', 'STORE' );
ok( !$obj3->can( 'foo' ), "... but it cannot 'foo'" );

is( $obj3->{a}, 1 );
is( $obj3->{b}[0], 1 );
is( $obj3->{b}[1], 2 );
is( $obj3->{b}[2], 3 );

is( $db3->{unblessed}{a}, 1 );
is( $db3->{unblessed}{b}[0], 1 );
is( $db3->{unblessed}{b}[1], 2 );
is( $db3->{unblessed}{b}[2], 3 );

undef $db;
undef $db2;
undef $db3;

{
    unlink 't/test2.db';
    my $db = DBM::Deep->new(
        file     => "t/test2.db",
        autobless => 1,
    );
    if ($db->error()) {
        die "ERROR: " . $db->error();
    }
    my $obj = bless {
        a => 1,
        b => [ 1 .. 3 ],
    }, 'Foo';

    $db->import( { blessed => $obj } );

    undef $db;

    $db = DBM::Deep->new(
        file     => "t/test2.db",
        autobless => 1,
    );
    if ($db->error()) {
        die "ERROR: " . $db->error();
    }

    my $blessed = $db->{blessed};
    isa_ok( $blessed, 'Foo' );
    is( $blessed->{a}, 1 );
}

{
	##
	# test blessing hash into short named class (Foo), then re-blessing into
	# longer named class (FooFoo) and replacing key in db file, then validating
	# content after that point in file to check for corruption.
	##
    unlink 't/test3.db';
    my $db = DBM::Deep->new(
        file     => "t/test3.db",
        autobless => 1,
    );
    if ($db->error()) {
        die "ERROR: " . $db->error();
    }

    my $obj = bless {}, 'Foo';

    $db->{blessed} = $obj;
    $db->{after} = "hello";
    
    my $obj2 = bless {}, 'FooFoo';
    
    $db->{blessed} = $obj2;

    is( $db->{after}, "hello" );
}

