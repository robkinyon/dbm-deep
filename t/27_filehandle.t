##
# DBM::Deep Test
##
use strict;
use Test::More tests => 9;
use Test::Exception;

use DBM::Deep;

open(FILE, "t/27_filehandle.t.db") || die("Can't open t/27_filehandle.t.db\n");

my $db;

# test if we can open and read a db using its filehandle

ok(($db = DBM::Deep->new(fh => *FILE)), "open db in filehandle");
ok($db->{hash}->{foo}->[1] eq 'b', "and get at stuff in the database");
throws_ok {
    $db->{foo} = 1;
} qr/Cannot write to a readonly filehandle/, "Can't write to a read-only filehandle";
ok( !$db->exists( 'foo' ), "foo doesn't exist" );

undef $db;
close(FILE);

# now the same, but with an offset into the file.  Use the database that's
# embedded in the test for the DATA filehandle.  First, find the database ...
open(FILE, "t/28_DATA.t") || die("Can't open t/28_DATA.t\n");
while(my $line = <FILE>) {
    last if($line =~ /^__DATA__/);
}
my $offset = tell(FILE);
close(FILE);

open(FILE, "t/28_DATA.t");
ok(($db = DBM::Deep->new(fh => *FILE, file_offset => $offset)), "open db in filehandle with offset");
ok($db->{hash}->{foo}->[1] eq 'b', "and get at stuff in the database");

ok( !$db->exists( 'foo' ), "foo doesn't exist yet" );
throws_ok {
    $db->{foo} = 1;
} qr/Cannot write to a readonly filehandle/, "Can't write to a read-only filehandle";
ok( !$db->exists( 'foo' ), "foo doesn't exist" );
