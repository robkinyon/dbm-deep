package TestBase;

use 5.6.0;

use strict;
use warnings;

use File::Path ();
use File::Temp ();
use Fcntl qw( :flock );

use base 'Test::Class';

use DBM::Deep;

sub setup_dir : Test(startup) {
    my $self = shift;

    $self->{workdir} ||= File::Temp::tempdir();

    return;
}

sub new_file {
    my $self = shift;

    $self->setup_dir;

    my ($fh, $filename) = File::Temp::tempfile(
        'tmpXXXX', DIR => $self->{workdir}, CLEANUP => 1,
    );
    flock( $fh, LOCK_UN );

    return $filename;
}

sub remove_dir : Test(shutdown) {
    my $self = shift;

    File::Path::rmtree( $self->{workdir} );

    return;
}

1;
__END__
