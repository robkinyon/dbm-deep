use Module::Build;

use strict;
use warnings FATAL => 'all';

my $class = Module::Build->subclass(
    class => "Module::Build::Custom",
    code => <<'SUBCLASS' );

sub ACTION_test {
    my $self = shift;
    if ( $self->notes('TEST_MYSQL_DSN') ) {
        $ENV{$_} = $self->notes($_) for qw(
            TEST_MYSQL_DSN TEST_MYSQL_USER TEST_MYSQL_PASS
        );
    }
    if ( $self->notes( 'LONG_TESTS' ) ) {
        $ENV{LONG_TESTS} = 1;
    }
    $self->SUPER::ACTION_test;
}
SUBCLASS

my $build = $class->new(
    module_name => 'DBM::Deep',
    license => 'perl',
    requires => {
        'perl'              => '5.006_000',
        'Fcntl'             => '0.01',
        'Scalar::Util'      => '1.14',
        'Digest::MD5'       => '1.00',
    },
    build_requires => {
        'File::Path'      => '0.01',
        'File::Temp'      => '0.01',
        'Pod::Usage'      => '1.3',
        'Test::More'      => '0.88',
        'Test::Deep'      => '0.095',
        'Test::Warn'      => '0.08',
        'Test::Exception' => '0.21',
        'IO::Scalar'      => '0.01',
    },
    create_makefile_pl => 'traditional',
    add_to_cleanup => [
        'META.yml', '*.bak', '*.gz', 'Makefile.PL', 'cover_db',
    ],
    test_files => 't/??_*.t',
    auto_features => {
        dbi_engine => {
            description => 'DBI support (mysql only so far)',
            requires => {
                'DBI'        => '1.5',
                'DBD::mysql' => '4.001',
            },
        },
    },
);

if ( $build->y_n( "Run the long-running tests", 'n' ) ) {
    $build->notes( 'LONG_TESTS' => 1 );
}

if ( $build->features( 'dbi_engine' ) ) {
    if ( $build->y_n( "Run the tests against the DBI engine (for MySQL only)?", 'n' ) ) {
        my ($dsn, $user, $pass) = ('') x 3;
        $dsn = $build->prompt( "\tWhat is the full DSN (for example 'dbi:mysql:test')" );
        if ( $dsn ) {
            $user = $build->prompt( "\tWhat is the username?" );
            if ( $user ) {
                $pass = $build->prompt( "\tWhat is the password?" );
            }
        }

        $build->notes( 'TEST_MYSQL_DSN'  => $dsn );
        $build->notes( 'TEST_MYSQL_USER' => $user );
        $build->notes( 'TEST_MYSQL_PASS' => $pass );
    }
}

$build->create_build_script;
