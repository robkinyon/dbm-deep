use Module::Build;

use strict;

my $build = Module::Build->new(
    module_name => 'DBM::Deep',
    license => 'perl',
    requires => {
        'Digest::MD5'  => '1.00',
        'Scalar::Util' => '1.18',
    },
    optional => {
    },
    build_requires => {
        'Test::More'      => '0.47',
        'Test::Exception' => '0.21',
    },
    create_makefile_pl => 'traditional',
    add_to_cleanup => [
        'META.yml', '*.bak', '*.gz', 'Makefile.PL', 't/test*.db', 'cover_db'
    ],
    test_files => 't/??_*.t',
);

$build->create_build_script;

