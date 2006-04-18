package DBM::Deep::File;

use 5.6.0;

use strict;
use warnings;

use Fcntl qw( :DEFAULT :flock :seek );

our $VERSION = '0.01';

sub new {
    my $class = shift;
    my ($args) = @_;

    my $self = bless {
        audit_fh           => undef,
        audit_file         => undef,
        autobless          => 1,
        autoflush          => undef,
        end                => 0,
        fh                 => undef,
        file               => undef,
        file_offset        => 0,
        locking            => undef,
        locked             => 0,
        filter_store_key   => undef,
        filter_store_value => undef,
        filter_fetch_key   => undef,
        filter_fetch_value => undef,

        # These are values that are not expected to be passed in through
        # $args. They are here for documentation purposes.
        transaction_id     => 0,
        transaction_offset => 0,
        base_db_obj        => undef,
    }, $class;

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }

    if ( $self->{fh} && !$self->{file_offset} ) {
        $self->{file_offset} = tell( $self->{fh} );
    }

    $self->open unless $self->{fh};

    if ( $self->{audit_file} && !$self->{audit_fh} ) {
        my $flags = O_WRONLY | O_APPEND | O_CREAT;

        my $fh;
        sysopen( $fh, $self->{audit_file}, $flags )
            or die "Cannot open audit file '$self->{audit_file}' for read/write: $!";

        # Set the audit_fh to autoflush
        my $old = select $fh;
        $|=1;
        select $old;

        $self->{audit_fh} = $fh;
    }


    return $self;
}

sub set_db {
    unless ( $_[0]{base_db_obj} ) {
        $_[0]{base_db_obj} = $_[1];
        Scalar::Util::weaken( $_[0]{base_db_obj} );
    }
}

sub open {
    my $self = shift;

    # Adding O_BINARY does remove the need for the binmode below. However,
    # I'm not going to remove it because I don't have the Win32 chops to be
    # absolutely certain everything will be ok.
    my $flags = O_RDWR | O_CREAT | O_BINARY;

    my $fh;
    sysopen( $fh, $self->{file}, $flags )
        or die "DBM::Deep: Cannot sysopen file '$self->{file}': $!\n";
    $self->{fh} = $fh;

    # Even though we use O_BINARY, better be safe than sorry.
    binmode $fh;

    if ($self->{autoflush}) {
        my $old = select $fh;
        $|=1;
        select $old;
    }

    return 1;
}

sub close {
    my $self = shift;

    if ( $self->{fh} ) {
        close $self->{fh};
        $self->{fh} = undef;
    }

    return 1;
}

sub DESTROY {
    my $self = shift;
    return unless $self;

    $self->close;

    return;
}

##
# If db locking is set, flock() the db file.  If called multiple
# times before unlock(), then the same number of unlocks() must
# be called before the lock is released.
##
sub lock {
    my $self = shift;
    my ($obj, $type) = @_;
    $type = LOCK_EX unless defined $type;

    if (!defined($self->{fh})) { return; }

    if ($self->{locking}) {
        if (!$self->{locked}) {
            flock($self->{fh}, $type);

            # refresh end counter in case file has changed size
            my @stats = stat($self->{fh});
            $self->{end} = $stats[7];

            # double-check file inode, in case another process
            # has optimize()d our file while we were waiting.
            if ($stats[1] != $self->{inode}) {
                $self->close;
                $self->open;

                #XXX This needs work
                $obj->{engine}->setup_fh( $obj );

                flock($self->{fh}, $type); # re-lock

                # This may not be necessary after re-opening
                $self->{end} = (stat($self->{fh}))[7]; # re-end
            }
        }
        $self->{locked}++;

        return 1;
    }

    return;
}

##
# If db locking is set, unlock the db file.  See note in lock()
# regarding calling lock() multiple times.
##
sub unlock {
    my $self = shift;

    if (!defined($self->{fh})) { return; }

    if ($self->{locking} && $self->{locked} > 0) {
        $self->{locked}--;
        if (!$self->{locked}) { flock($self->{fh}, LOCK_UN); }

        return 1;
    }

    return;
}

sub set_transaction_offset {
    my $self = shift;
    $self->{transaction_offset} = shift;
}

sub begin_transaction {
    my $self = shift;

    my $fh = $self->{fh};

    $self->lock;

    seek( $fh, $self->{transaction_offset}, SEEK_SET );
    my $buffer;
    read( $fh, $buffer, 4 );
    $buffer = unpack( 'N', $buffer );

    for ( 1 .. 32 ) {
        next if $buffer & (1 << ($_ - 1));
        $self->{transaction_id} = $_;
        $buffer |= (1 << $_-1 );
        last;
    }

    seek( $fh, $self->{transaction_offset}, SEEK_SET );
    print( $fh pack( 'N', $buffer ) );

    $self->unlock;

    return $self->{transaction_id};
}

sub end_transaction {
    my $self = shift;

    my $fh = $self->{fh};

    $self->lock;

    seek( $fh, $self->{transaction_offset}, SEEK_SET );
    my $buffer;
    read( $fh, $buffer, 4 );
    $buffer = unpack( 'N', $buffer );

    # Unset $self->{transaction_id} bit

    seek( $fh, $self->{transaction_offset}, SEEK_SET );
    print( $fh pack( 'N', $buffer ) );

    $self->unlock;

    $self->{transaction_id} = 0;
}

sub current_transactions {
    my $self = shift;

    my $fh = $self->{fh};

    $self->lock;

    seek( $fh, $self->{transaction_offset}, SEEK_SET );
    my $buffer;
    read( $fh, $buffer, 4 );
    $buffer = unpack( 'N', $buffer );

    $self->unlock;

    my @transactions;
    for ( 1 .. 32 ) {
        if ( $buffer & (1 << ($_ - 1)) ) {
            push @transactions, $_;
        }
    }

    return grep { $_ != $self->{transaction_id} } @transactions;
}

sub transaction_id { return $_[0]->{transaction_id} }

#sub commit {
#}

1;
__END__

