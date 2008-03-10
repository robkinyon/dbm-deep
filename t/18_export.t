##
# DBM::Deep Test
##
use strict;
use Test::More tests => 6;
use Test::Deep;
use t::common qw( new_fh );

use_ok( 'DBM::Deep' );

my %struct = (
    key1 => "value1",
    key2 => "value2",
    array1 => [ "elem0", "elem1", "elem2", { foo => 'bar' }, [ 5 ] ],
    hash1 => {
        subkey1 => "subvalue1",
        subkey2 => "subvalue2",
        subkey3 => bless( {
            sub_obj => bless([
                bless([], 'Foo'),
            ], 'Foo'),
            sub_obj2 => bless([], 'Foo'),
        }, 'Foo' ),
    },
);

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new({
    file      => $filename,
    fh => $fh,
    autobless => 1,
});

##
# Create structure in DB
##
$db->import( \%struct );

##
# Export entire thing
##
my $compare = $db->export();

cmp_deeply(
    $compare,
    {
        key1 => "value1",
        key2 => "value2",
        array1 => [ "elem0", "elem1", "elem2", { foo => 'bar' }, [ 5 ] ],
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2",
            subkey3 => bless( {
                sub_obj => bless([
                    bless([], 'Foo'),
                ], 'Foo'),
                sub_obj2 => bless([], 'Foo'),
            }, 'Foo' ),
        },
    },
    "Everything matches",
);

isa_ok( tied(%{$db->{hash1}{subkey3}})->export, 'Foo' );
isa_ok( tied(@{$db->{hash1}{subkey3}{sub_obj}})->export, 'Foo' );
isa_ok( tied(@{$db->{hash1}{subkey3}{sub_obj}[0]})->export, 'Foo' );
isa_ok( tied(@{$db->{hash1}{subkey3}{sub_obj2}})->export, 'Foo' );
