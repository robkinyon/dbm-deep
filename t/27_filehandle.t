use 5.006_000;

use strict;
use warnings FATAL => 'all';

use Test::More tests => 14;
use Test::Exception;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

{
    my ($fh, $filename) = new_fh();

    # Create the datafile to be used
    {
        my $db = DBM::Deep->new( $filename );
        $db->{hash} = { foo => [ 'a' .. 'c' ] };
    }

    {
        open(my $fh, '<', $filename) || die("Can't open '$filename' for reading: $!\n");

        # test if we can open and read a db using its filehandle

        my $db;
        ok( ($db = DBM::Deep->new( fh => $fh )), "open db in filehandle" );
        ok( $db->{hash}{foo}[1] eq 'b', "and get at stuff in the database" );
        throws_ok {
            $db->{foo} = 1;
        } qr/Cannot write to a readonly filehandle/, "Can't write to a read-only filehandle";
        ok( !$db->exists( 'foo' ), "foo doesn't exist" );

        SKIP: {
            skip( "No inode tests on Win32", 1 )
                if ( $^O eq 'MSWin32' || $^O eq 'cygwin' );
            my $db_obj = $db->_get_self;
            ok( $db_obj->_engine->storage->{inode}, "The inode has been set" );
        }

        close($fh);
    }
}

# now the same, but with an offset into the file.  Use the database that's
# embedded in the test for the DATA filehandle.  First, find the database ...
{
    my ($fh,$filename) = new_fh();

    print $fh "#!$^X\n";
    print $fh <<'__END_FH__';
use strict;
use Test::More 'no_plan';
Test::More->builder->no_ending(1);
Test::More->builder->{Curr_Test} = 12;

use_ok( 'DBM::Deep' );

my $db = DBM::Deep->new({
    fh => *DATA,
});
is($db->{x}, 'b', "and get at stuff in the database");
__END_FH__
    print $fh "__DATA__\n";
    close $fh;

    my $offset = do {
        open my $fh, '<', $filename;
        while(my $line = <$fh>) {
            last if($line =~ /^__DATA__/);
        }
        tell($fh);
    };

    {
        my $db = DBM::Deep->new({
            file        => $filename,
            file_offset => $offset,
#XXX For some reason, this is needed to make the test pass. Figure out why later.
locking => 0,
        });

        $db->{x} = 'b';
        is( $db->{x}, 'b', 'and it was stored' );
    }

    {
        open my $fh, '<', $filename;
        my $db = DBM::Deep->new({
            fh          => $fh,
            file_offset => $offset,
        });

        is($db->{x}, 'b', "and get at stuff in the database");

        ok( !$db->exists( 'foo' ), "foo doesn't exist yet" );
        throws_ok {
            $db->{foo} = 1;
        } qr/Cannot write to a readonly filehandle/, "Can't write to a read-only filehandle";
        ok( !$db->exists( 'foo' ), "foo still doesn't exist" );

        is( $db->{x}, 'b' );
    }

    exec( "$^X -Iblib/lib $filename" );
}
