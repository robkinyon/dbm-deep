use strict;
use warnings FATAL => 'all';

use Test::More;
use Test::Deep;
use Test::Exception;
use Test::Warn;
use lib 't';
use common qw( new_dbm );

use_ok( 'DBM::Deep' );

my $dbm_factory = new_dbm(
    locking => 1,
    autoflush => 1,
);
while ( my $dbm_maker = $dbm_factory->() ) {
    my $db1 = $dbm_maker->();
    ok(!$db1->supports('transactions'), "num_txns<=1 means transactions is not supported");
    
    local $TODO;
    if (ref $db1->_get_self->{engine} eq 'DBM::Deep::Engine::DBI') {
        $TODO = 'DBM transactions not yet implemented';
    }

    throws_ok {
        $db1->begin_work;
    } qr/Cannot begin_work unless transactions are supported/, "Attempting to begin_work without a transactions supported throws an error";
    throws_ok {
        $db1->rollback;
    } qr/Cannot rollback unless transactions are supported/, "Attempting to rollback without a transactions supported throws an error";
    throws_ok {
        $db1->commit;
    } qr/Cannot commit unless transactions are supported/, "Attempting to commit without a transactions supported throws an error";

    warning_like {
        $dbm_maker->(num_txns => 2);
    } qr/num_txns \(2\) is different from the file \(1\)/, "Opening a file with a different num_txns throws a warnings";
}

done_testing
