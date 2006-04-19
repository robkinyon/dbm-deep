use strict;
use warnings;

{
    # This is here because Tie::File is STOOPID.

    package My::Tie::File;
    sub TIEARRAY {
        my $class = shift;
        my ($filename) = @_;

        return bless {
            filename => $filename,
        }, $class;
    }

    sub FETCH {
        my $self = shift;
        my ($idx) = @_;

        open( my $fh, $self->{filename} );
        my @x = <$fh>;
        close $fh;

        return $x[$idx];
    }

    sub FETCHSIZE {
        my $self = shift;

        open( my $fh, $self->{filename} );
        my @x = <$fh>;
        close $fh;

        return scalar @x;
    }

    sub STORESIZE {}
}

use Test::More tests => 24;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my ($audit_fh, $audit_file) = new_fh();

my @audit;
tie @audit, 'My::Tie::File', $audit_file;

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new({
    file       => $filename,
    audit_file => $audit_file,
    #autuflush  => 1,
});
isa_ok( $db, 'DBM::Deep' );

like(
    $audit[0], qr/^\# Database created on/,
    "Audit file header written to",
);

$db->{foo} = 'bar';
like( $audit[1], qr{^\$db->{foo} = 'bar';}, "Basic assignment correct" );

$db->{foo} = 'baz';
like( $audit[2], qr{^\$db->{foo} = 'baz';}, "Basic update correct" );

$db->{bar} = { a => 1 };
like( $audit[3], qr{\$db->\{bar\} = \{\};}, "Hash assignment correct" );
like( $audit[4], qr{\$db->\{bar\}\{a\} = '1';}, "... child 1 good" );

$db->{baz} = [ 1 .. 2 ];
like( $audit[5], qr{\$db->{baz} = \[\];}, "Array assignment correct" );
like( $audit[6], qr{\$db->{baz}\[0\] = '1';}, "... child 1 good" );
like( $audit[7], qr{\$db->{baz}\[1\] = '2';}, "... child 2 good" );

{
    my $v = $db->{baz};
    $v->[5] = [ 3 .. 5 ];
    like( $audit[8], qr{\$db->{baz}\[5\] = \[\];}, "Child array assignment correct" );
    like( $audit[9], qr{\$db->{baz}\[5\]\[0\] = '3';}, "... child 1 good" );
    like( $audit[10], qr{\$db->{baz}\[5]\[1] = '4';}, "... child 2 good" );
    like( $audit[11], qr{\$db->{baz}\[5]\[2] = '5';}, "... child 3 good" );
}

undef $db;

$db = DBM::Deep->new({
    file => $filename,
    audit_file => $audit_file,
});

$db->{new} = 9;
like( $audit[12], qr{\$db->{new} = '9';}, "Writing after closing the file works" );

my $export = $db->export;
undef $db;

{
    my ($fh2, $file2) = new_fh();
    my $db = DBM::Deep->new({
        file => $file2,
    });

    for ( @audit ) {
        eval "$_";
    }

    my $export2 = $db->export;

    is_deeply( $export2, $export, "And recovery works" );
}

{
    $db = DBM::Deep->new({
        file => $filename,
        audit_file => $audit_file,
    });

    delete $db->{baz};
    like( $audit[13], qr{delete \$db->{baz};}, "Deleting works" );

    $export = $db->export;
}

{
    my ($fh2, $file2) = new_fh();
    my $db = DBM::Deep->new({
        file => $file2,
    });

    for ( @audit ) {
        eval "$_";
    }

    my $export2 = $db->export;

    is_deeply( $export2, $export, "And recovery works" );
}

SKIP: {
    skip 'Not done yet', 1;
    $db = DBM::Deep->new({
        file => $filename,
        audit_file => $audit_file,
    });

    $db->{bar}->clear;
    like( $audit[14], qr{\$db->{bar} = \{\};}, "Clearing works" );

    $export = $db->export;
}

{
    my ($fh2, $file2) = new_fh();
    my $db = DBM::Deep->new({
        file => $file2,
    });

    for ( @audit ) {
        eval "$_";
    }

    my $export2 = $db->export;

    is_deeply( $export2, $export, "And recovery works" );
}

SKIP: {
    skip "Not working", 3;
    $db = DBM::Deep->new({
        file => $filename,
        audit_file => $audit_file,
    });

    $db->{blessed} = bless { a => 5, b => 3 }, 'Floober';
    like( $audit[15], qr{\$db->{blessed} = bless {}, 'Floober';},
            "Assignment of a blessed reference works" );
    like( $audit[16], qr{\$db->{blessed}{a} = '5';}, "... child 1" );
    like( $audit[17], qr{\$db->{blessed}{b} = '3';}, "... child 2" );

    $export = $db->export;
}

{
    my ($fh2, $file2) = new_fh();
    my $db = DBM::Deep->new({
        file => $file2,
    });

    for ( @audit ) {
        eval "$_";
    }

    my $export2 = $db->export;

    is_deeply( $export2, $export, "And recovery works" );
}
