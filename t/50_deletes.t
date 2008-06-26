
use strict;
use Test;
use DBM::Deep;
use t::common qw( new_fh );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( file => $filename, fh => $fh, );

my $max = 10;

plan tests => $max;

my $x = 0;
while( $x < $max ) {
    eval {
        delete $db->{borked}{test};
        $db->{borked}{test} = 1;
    };

    ok($@, '');
    $x++;
}
