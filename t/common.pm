package # Hide from PAUSE
    t::common;

use strict;
use warnings FATAL => 'all';

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

sub new_fh {
    my ($fh, $filename) = tempfile( 'tmpXXXX', DIR => $dir, UNLINK => 1 );

    # This is because tempfile() returns a flock'ed $fh on MacOSX.
    flock $fh, LOCK_UN;

    return ($fh, $filename);
}

sub new_dbm {
    my @args = @_;
    my ($fh, $filename) = new_fh();

    my @reset_funcs;
    my @extra_args;

    unless ( $ENV{NO_TEST_FILE} ) {
        push @reset_funcs, undef;
        push @extra_args, (
            [ file => $filename ],
        );
    }

#    eval { require DBD::SQLite; };
#    unless ( $@ ) {
#        push @extra_args, [
#        ];
#    }

    if ( $ENV{TEST_MYSQL_DSN} ) {
        push @reset_funcs, sub {
            my $dbh = DBI->connect(
                "dbi:mysql:$ENV{TEST_MYSQL_DSN}",
                $ENV{TEST_MYSQL_USER},
                $ENV{TEST_MYSQL_PASS},
            );
            my $sql = do {
                my $filename = 'etc/mysql_tables.sql';
                open my $fh, '<', $filename
                    or die "Cannot open '$filename' for reading: $!\n";
                local $/;
                <$fh>
            };
            foreach my $line ( split ';', $sql ) {
                $dbh->do( "$line" ) if $line =~ /\S/;
            }
        };
        push @extra_args, [
            dbi => {
                dsn      => "dbi:mysql:$ENV{TEST_MYSQL_DSN}",
                user     => $ENV{TEST_MYSQL_USER},
                password => $ENV{TEST_MYSQL_PASS},
            },
        ];
    }

    return sub {
        return unless @extra_args;
        my @these_args = @{ shift @extra_args };
        if ( my $reset = shift @reset_funcs ) {
            $reset->();
        }
        return sub {
            DBM::Deep->new(
                @these_args, @args, @_,
            );
        };
    };
}

1;
__END__
