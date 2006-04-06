package t::common;

use 5.6.0;

use strict;
use warnings;

our $VERSION = '0.01';

use base 'Exporter';
our @EXPORT_OK = qw(
    new_fh
);

use File::Spec ();
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

my $parent = $ENV{WORK_DIR} || File::Spec->tmpdir;
my $dir = tempdir( CLEANUP => 1, DIR => $parent );

sub new_fh {
    my ($fh, $filename) = tempfile( 'tmpXXXX', DIR => $dir );

    # This is because tempfile() returns a flock'ed $fh on MacOSX.
    flock $fh, LOCK_UN;

    return ($fh, $filename);
}
#END{<>}
1;
__END__

