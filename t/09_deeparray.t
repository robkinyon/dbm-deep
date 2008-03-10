##
# DBM::Deep Test
##
use strict;
use Test::More;

plan skip_all => "You must set \$ENV{LONG_TESTS} to run the long tests"
    unless $ENV{LONG_TESTS};

plan tests => 3;
use t::common qw( new_fh );

diag "This test can take up to a minute to run. Please be patient.";

use_ok( 'DBM::Deep' );

my ($fh, $filename) = new_fh();

my $max_levels = 1000;

{
    my $db = DBM::Deep->new(
        file => $filename,
        fh => $fh,
        type => DBM::Deep->TYPE_ARRAY,
    );

    $db->[0] = [];
    my $temp_db = $db->[0];
    for my $k ( 0 .. $max_levels ) {
        $temp_db->[$k] = [];
        $temp_db = $temp_db->[$k];
    }
    $temp_db->[0] = "deepvalue";
}

{
    open $fh, '+<', $filename;
    my $db = DBM::Deep->new(
        file => $filename,
        fh => $fh,
        type => DBM::Deep->TYPE_ARRAY,
    );

    my $cur_level = -1;
    my $temp_db = $db->[0];
    for my $k ( 0 .. $max_levels ) {
        $cur_level = $k;
        $temp_db = $temp_db->[$k];
        eval { $temp_db->isa( 'DBM::Deep' ) } or last;
    }
    is( $cur_level, $max_levels, "We read all the way down to level $cur_level" );
    is( $temp_db->[0], "deepvalue", "And we retrieved the value at the bottom of the ocean" );
}
