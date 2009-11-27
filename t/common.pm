package # Hide from PAUSE
    t::common;

use 5.006_000;

use strict;
use warnings;

our $VERSION = '0.01';

use base 'Exporter';
our @EXPORT_OK = qw(
    new_dbm
    new_fh
);

use File::Spec ();
use File::Temp qw( tempfile tempdir );
use Fcntl qw( :flock );

my $parent = $ENV{WORK_DIR} || File::Spec->tmpdir;
my $dir = tempdir( CLEANUP => 1, DIR => $parent );
#my $dir = tempdir( DIR => '.' );

sub new_fh {
    my ($fh, $filename) = tempfile( 'tmpXXXX', DIR => $dir, UNLINK => 1 );

    # This is because tempfile() returns a flock'ed $fh on MacOSX.
    flock $fh, LOCK_UN;

    return ($fh, $filename);
}

sub new_dbm {
    my @args = @_;
    my ($fh, $filename) = new_fh();
    my @extra_args = (
        [ file => $filename ],
    );
    return sub {
        return unless @extra_args;
        my @these_args = @{ shift @extra_args };
        return sub {
            DBM::Deep->new(
                @these_args, @args, @_,
            );
        };
    };
}

1;
__END__
