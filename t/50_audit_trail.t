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

sub testit {
    my ($db_orig, $audit) = @_;
    my $export = $db_orig->export;

    my ($fh2, $file2) = new_fh();
    my $db = DBM::Deep->new({
        file => $file2,
    });

    for ( @$audit ) {
        eval "$_";
    }

    my $export2 = $db->export;

    cmp_deeply( $export2, $export, "And recovery works" );
}

use Test::More tests => 12;
use Test::Deep;
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
testit( $db, \@audit );

$db->{foo} = 'baz';
testit( $db, \@audit );

$db->{bar} = { a => 1 };
testit( $db, \@audit );

$db->{baz} = [ 1 .. 2 ];
testit( $db, \@audit );

{
    my $v = $db->{baz};
    $v->[5] = [ 3 .. 5 ];
    testit( $db, \@audit );
}

undef $db;

$db = DBM::Deep->new({
    file => $filename,
    audit_file => $audit_file,
});

$db->{new} = 9;
testit( $db, \@audit );

delete $db->{baz};
testit( $db, \@audit );

$db->{bar}->clear;
testit( $db, \@audit );

$db->{blessed} = bless { a => 5, b => 3 }, 'Floober';
testit( $db, \@audit );
