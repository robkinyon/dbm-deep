use Module::Build 0.28; # prepare_metadata

use strict;

my $build = Module::Build->subclass(
    class => 'Any::Thing',
    code => '
        sub prepare_metadata {
            my $node = shift->SUPER::prepare_metadata(@_);
            my $ver = $node->{version};
            $_->{version} = $ver for values %{$node->{provides}};
            $node;
        }
   ',
)->new(
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
        'Test::Deep'      => '0.095',
        'Test::Warn'      => '0.08',
        'Test::More'      => '0.88', # done_testing
        'Test::Exception' => '0.21',
        'IO::Scalar'      => '0.01',
    },
    create_makefile_pl => 'traditional',
    add_to_cleanup => [
        'META.yml', '*.bak', '*.gz', 'Makefile.PL', 'cover_db',
    ],
    test_files => 't/??_*.t',
);

$build->create_build_script;
