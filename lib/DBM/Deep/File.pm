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
        autobless          => undef,
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

        transaction_id     => 0,
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

    return $self;
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

sub begin_transaction {
    my $self = shift;

    $self->{transaction_id}++;
}

sub end_transaction {
    my $self = shift;

    $self->{transaction_id} = 0;
}

sub transaction_id {
    my $self = shift;

    return $self->{transaction_id};
}

#sub commit {
#}

1;
__END__

