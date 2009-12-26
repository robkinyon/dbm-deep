package DBM::Deep::Storage::DBI;

use 5.006_000;

use strict;
use warnings FATAL => 'all';

use base 'DBM::Deep::Storage';

use DBI;

sub new {
    my $class = shift;
    my ($args) = @_;

    my $self = bless {
        autobless => 1,
        dbh       => undef,
        dbi       => undef,
    }, $class;

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }

    $self->open unless $self->{dbh};

    return $self;
}

sub open {
    my $self = shift;

    # TODO: Is this really what should happen?
    return if $self->{dbh};

    $self->{dbh} = DBI->connect(
        $self->{dbi}{dsn}, $self->{dbi}{username}, $self->{dbi}{password}, {
            AutoCommit => 0,
            PrintError => 0,
            RaiseError => 1,
            %{ $self->{dbi}{connect_args} || {} },
        },
    ) or die $DBI::error;

    return 1;
}

sub close {
    my $self = shift;
    $self->{dbh}->disconnect if $self->{dbh};
    return 1;
}

sub DESTROY {
    my $self = shift;
    $self->close if ref $self;
}

# Is there a portable way of determining writability to a DBH?
sub is_writable {
    my $self = shift;
    return 1;
}

sub lock_exclusive {
    my $self = shift;
}

sub lock_shared {
    my $self = shift;
}

sub unlock {
    my $self = shift;
}

sub read_from {
    my $self = shift;
    my ($table, $cond, @cols) = @_;

    $cond = { id => $cond } unless ref $cond;

    my @keys = keys %$cond;
    my $where = join ' AND ', map { "`$_` = ?" } @keys;

    return $self->{dbh}->selectall_arrayref(
        "SELECT `@{[join '`,`', @cols ]}` FROM $table WHERE $where",
        { Slice => {} }, @{$cond}{@keys},
    );
}

sub flush {}

sub write_to {
    my $self = shift;
    my ($table, $id, %args) = @_;

    my @keys = keys %args;
    my $sql =
        "REPLACE INTO $table ( `id`, "
          . join( ',', map { "`$_`" } @keys )
      . ") VALUES ("
          . join( ',', ('?') x (@keys + 1) )
      . ")";
    $self->{dbh}->do( $sql, undef, $id, @args{@keys} );

    return $self->{dbh}{mysql_insertid};
}

sub delete_from {
    my $self = shift;
    my ($table, $cond) = @_;

    $cond = { id => $cond } unless ref $cond;

    my @keys = keys %$cond;
    my $where = join ' AND ', map { "`$_` = ?" } @keys;

    $self->{dbh}->do(
        "DELETE FROM $table WHERE $where", undef, @{$cond}{@keys},
    );
}

1;
__END__
