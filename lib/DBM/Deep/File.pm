package DBM::Deep::File;

use 5.6.0;

use strict;
use warnings;

our $VERSION = q(0.99_01);

use Fcntl qw( :DEFAULT :flock :seek );

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
        transaction_audit  => undef,
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
    my $self = shift;

    unless ( $self->{base_db_obj} ) {
        $self->{base_db_obj} = shift;
        Scalar::Util::weaken( $self->{base_db_obj} );
    }

    return;
}

sub open {
    my $self = shift;

    # Adding O_BINARY should remove the need for the binmode below. However,
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

sub set_inode {
    my $self = shift;

    unless ( $self->{inode} ) {
        my @stats = stat($self->{fh});
        $self->{inode} = $stats[1];
        $self->{end} = $stats[7];
    }

    return 1;
}

sub print_at {
    my $self = shift;
    my $loc  = shift;

    local ($/,$\);

    my $fh = $self->{fh};
    if ( defined $loc ) {
        seek( $fh, $loc + $self->{file_offset}, SEEK_SET );
    }

    print( $fh @_ );

    return 1;
}

sub read_at {
    my $self = shift;
    my ($loc, $size) = @_;

    local ($/,$\);

    my $fh = $self->{fh};
    if ( defined $loc ) {
        seek( $fh, $loc + $self->{file_offset}, SEEK_SET );
    }

    my $buffer;
    read( $fh, $buffer, $size);

    return $buffer;
}

sub increment_pointer {
    my $self = shift;
    my ($size) = @_;

    if ( defined $size ) {
        seek( $self->{fh}, $size, SEEK_CUR );
    }

    return 1;
}

sub DESTROY {
    my $self = shift;
    return unless $self;

    $self->close;

    return;
}

sub request_space {
    my $self = shift;
    my ($size) = @_;

    #XXX Do I need to reset $self->{end} here? I need a testcase
    my $loc = $self->{end};
    $self->{end} += $size;

    return $loc;
}

#sub release_space {
#    my $self = shift;
#    my ($size, $loc) = @_;
#
#    local($/,$\);
#
#    my $next_loc = 0;
#
#    my $fh = $self->{fh};
#    seek( $fh, $loc + $self->{file_offset}, SEEK_SET );
#    print( $fh SIG_FREE
#        . pack($self->{long_pack}, $size )
#        . pack($self->{long_pack}, $next_loc )
#    );
#
#    return;
#}

##
# If db locking is set, flock() the db file.  If called multiple
# times before unlock(), then the same number of unlocks() must
# be called before the lock is released.
##
sub lock {
    my $self = shift;
    my ($obj, $type) = @_;

    #XXX This may not always be the correct thing to do
    $obj = $self->{base_db_obj} unless defined $obj;

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

sub audit {
    my $self = shift;
    my ($string) = @_;

    if ( my $afh = $self->{audit_fh} ) {
        flock( $afh, LOCK_EX );

        if ( $string =~ /^#/ ) {
            print( $afh "$string " . localtime(time) . "\n" );
        }
        else {
            print( $afh "$string # " . localtime(time) . "\n" );
        }

        flock( $afh, LOCK_UN );
    }

    if ( $self->{transaction_audit} ) {
        push @{$self->{transaction_audit}}, $string;
    }

    return 1;
}

sub begin_transaction {
    my $self = shift;

    my $fh = $self->{fh};

    $self->lock;

    my $buffer = $self->read_at( $self->{transaction_offset}, 4 );
    my ($next, @trans) = unpack( 'C C C C C C C C C C C C C C C C', $buffer );

    $self->{transaction_id} = ++$next;

    die if $trans[-1] != 0;

    for ( my $i = 0; $i <= $#trans; $i++ ) {
        next if $trans[$i] != 0;
        $trans[$i] = $next;
        last;
    }

    $self->print_at(
        $self->{transaction_offset},
        pack( 'C C C C C C C C C C C C C C C C', $next, @trans),
    );

    $self->unlock;

    $self->{transaction_audit} = [];

    return $self->{transaction_id};
}

sub end_transaction {
    my $self = shift;

    my $fh = $self->{fh};

    $self->lock;

    my $buffer = $self->read_at( $self->{transaction_offset}, 4 );
    my ($next, @trans) = unpack( 'C C C C C C C C C C C C C C C C', $buffer );

    @trans = grep { $_ != $self->{transaction_id} } @trans;

    $self->print_at(
        $self->{transaction_offset},
        pack( 'C C C C C C C C C C C C C C C C', $next, @trans),
    );

    #XXX Need to free the space used by the current transaction

    $self->unlock;

    $self->{transaction_id} = 0;
    $self->{transaction_audit} = undef;

#    $self->{base_db_obj}->optimize;
#    $self->{inode} = undef;
#    $self->set_inode;

    return 1;
}

sub current_transactions {
    my $self = shift;

    my $fh = $self->{fh};

    $self->lock;

    my $buffer = $self->read_at( $self->{transaction_offset}, 4 );
    my ($next, @trans) = unpack( 'C C C C C C C C C C C C C C C C', $buffer );

    $self->unlock;

    return grep { $_ && $_ != $self->{transaction_id} } @trans;
}

sub transaction_id { return $_[0]->{transaction_id} }

sub commit_transaction {
    my $self = shift;

    my @audit = @{$self->{transaction_audit}};

    $self->end_transaction;

    {
        my $db = $self->{base_db_obj};
        for ( @audit ) {
            eval "$_;";
            warn "$_: $@\n" if $@;
        }
    }

    return 1;
}

1;
__END__

