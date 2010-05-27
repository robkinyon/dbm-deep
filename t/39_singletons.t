use strict;
use warnings FATAL => 'all';

use Test::More;
use Test::Deep;
use t::common qw( new_dbm );

sub is_undef {
 ok(!defined $_[0] || ref $_[0] eq 'DBM::Deep::Null', $_[1])
}

use_ok( 'DBM::Deep' );

my $dbm_factory = new_dbm(
    locking => 1,
    autoflush => 1,
);
while ( my $dbm_maker = $dbm_factory->() ) {
    my $db = $dbm_maker->();

    SKIP: {
        skip "This engine doesn't support singletons", 8
            unless $db->supports( 'singletons' );

        $db->{a} = 1;
        $db->{foo} = { a => 'b' };
        my $x = $db->{foo};
        my $y = $db->{foo};

        is( $x, $y, "The references are the same" );

        delete $db->{foo};
        is_undef( $x, "After deleting the DB location, external references are also undef (\$x)" );
        is_undef( $y, "After deleting the DB location, external references are also undef (\$y)" );
        is( eval { $x + 0 }, undef, "DBM::Deep::Null can be added to." );
        is( eval { $y + 0 }, undef, "DBM::Deep::Null can be added to." );
        is_undef( $db->{foo}, "The {foo} location is also undef." );

        # These shenanigans work to get another hashref
        # into the same data location as $db->{foo} was.
        $db->{foo} = {};
        delete $db->{foo};
        $db->{foo} = {};
        $db->{bar} = {};

        is_undef( $x, "After re-assigning to {foo}, external references to old values are still undef (\$x)" );
        is_undef( $y, "After re-assigning to {foo}, external references to old values are still undef (\$y)" );

        my($w,$line);
        my $file = __FILE__;
        local $SIG{__WARN__} = sub { $w = $_[0] };
        eval {
            $line = __LINE__;   $db->{stext} = $x;
        };
        is $@, "Assignment of stale reference at $file line $line.\n",
            'assigning a stale reference to the DB dies w/FATAL warnings';
        {
            no warnings FATAL => "all";
            use warnings 'uninitialized'; # non-FATAL
            $db->{stext} = $x;     $line = __LINE__;
            is $w, "Assignment of stale reference at $file line $line.\n",
              'assigning a stale reference back to the DB warns';
        }
        {
            no warnings 'uninitialized';
            $w = undef;
            $db->{stext} = $x;
            is $w, undef,
              'stale ref assignment warnings can be suppressed';
        }

	eval {                   $line = __LINE__+1;
          () = $x->{stit};
        };
        like $@,
          qr/^Can't use a stale reference as a HASH at \Q$file\E line(?x:
             ) $line\.?\n\z/,
          'Using a stale reference as a hash dies';
	eval {                   $line = __LINE__+1;
          () = $x->[28];
        };
        like $@,
          qr/^Can't use a stale reference as an ARRAY at $file line (?x:
             )$line\.?\n\z/,
          'Using a stale reference as an array dies';
    }
}

{
 my $null = bless {}, 'DBM::Deep::Null';
 cmp_ok $null, 'eq', undef, 'DBM::Deep::Null compares equal to undef';
 cmp_ok $null, '==', undef, 'DBM::Deep::Null compares ==ual to undef';
}

SKIP: {
    skip "What do we do with external references and txns?", 2;

    my $dbm_factory = new_dbm(
        locking   => 1,
        autoflush => 1,
        num_txns  => 2,
    );
    while ( my $dbm_maker = $dbm_factory->() ) {
        my $db = $dbm_maker->();

        $db->{foo} = { a => 'b' };
        my $x = $db->{foo};

        $db->begin_work;
    
            $db->{foo} = { c => 'd' };
            my $y = $db->{foo};

            # XXX What should happen here with $x and $y?
            is( $x, $y );
            is( $x->{c}, 'd' );

        $db->rollback;
    }
}

done_testing;
