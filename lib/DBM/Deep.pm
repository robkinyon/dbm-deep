package DBM::Deep;

##
# DBM::Deep
#
# Description:
#    Multi-level database module for storing hash trees, arrays and simple
#    key/value pairs into FTP-able, cross-platform binary database files.
#
#    Type `perldoc DBM::Deep` for complete documentation.
#
# Usage Examples:
#    my %db;
#    tie %db, 'DBM::Deep', 'my_database.db'; # standard tie() method
#
#    my $db = new DBM::Deep( 'my_database.db' ); # preferred OO method
#
#    $db->{my_scalar} = 'hello world';
#    $db->{my_hash} = { larry => 'genius', hashes => 'fast' };
#    $db->{my_array} = [ 1, 2, 3, time() ];
#    $db->{my_complex} = [ 'hello', { perl => 'rules' }, 42, 99 ];
#    push @{$db->{my_array}}, 'another value';
#    my @key_list = keys %{$db->{my_hash}};
#    print "This module " . $db->{my_complex}->[1]->{perl} . "!\n";
#
# Copyright:
#    (c) 2002-2006 Joseph Huckaby.  All Rights Reserved.
#    This program is free software; you can redistribute it and/or
#    modify it under the same terms as Perl itself.
##

use 5.006_000;

use strict;
use warnings;

our $VERSION = q(0.99_04);

use Fcntl qw( :flock );

use Clone ();
use Digest::MD5 ();
use FileHandle::Fmode ();
use Scalar::Util ();

use DBM::Deep::Engine;
use DBM::Deep::File;

##
# Setup constants for users to pass to new()
##
sub TYPE_HASH   () { DBM::Deep::Engine->SIG_HASH  }
sub TYPE_ARRAY  () { DBM::Deep::Engine->SIG_ARRAY }

# This is used in all the children of this class in their TIE<type> methods.
sub _get_args {
    my $proto = shift;

    my $args;
    if (scalar(@_) > 1) {
        if ( @_ % 2 ) {
            $proto->_throw_error( "Odd number of parameters to " . (caller(1))[2] );
        }
        $args = {@_};
    }
    elsif ( ref $_[0] ) {
        unless ( eval { local $SIG{'__DIE__'}; %{$_[0]} || 1 } ) {
            $proto->_throw_error( "Not a hashref in args to " . (caller(1))[2] );
        }
        $args = $_[0];
    }
    else {
        $args = { file => shift };
    }

    return $args;
}

sub new {
    ##
    # Class constructor method for Perl OO interface.
    # Calls tie() and returns blessed reference to tied hash or array,
    # providing a hybrid OO/tie interface.
    ##
    my $class = shift;
    my $args = $class->_get_args( @_ );

    ##
    # Check if we want a tied hash or array.
    ##
    my $self;
    if (defined($args->{type}) && $args->{type} eq TYPE_ARRAY) {
        $class = 'DBM::Deep::Array';
        require DBM::Deep::Array;
        tie @$self, $class, %$args;
    }
    else {
        $class = 'DBM::Deep::Hash';
        require DBM::Deep::Hash;
        tie %$self, $class, %$args;
    }

    return bless $self, $class;
}

# This initializer is called from the various TIE* methods. new() calls tie(),
# which allows for a single point of entry.
sub _init {
    my $class = shift;
    my ($args) = @_;

    $args->{storage} = DBM::Deep::File->new( $args )
        unless exists $args->{storage};

    # locking implicitly enables autoflush
    if ($args->{locking}) { $args->{autoflush} = 1; }

    # These are the defaults to be optionally overridden below
    my $self = bless {
        type        => TYPE_HASH,
        base_offset => undef,
        staleness   => undef,

        storage     => undef,
        engine      => undef,
    }, $class;

    $args->{engine} = DBM::Deep::Engine->new( { %{$args}, obj => $self } )
        unless exists $args->{engine};

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }

    eval {
      local $SIG{'__DIE__'};

      $self->lock;
      $self->_engine->setup_fh( $self );
      $self->_storage->set_inode;
      $self->unlock;
    }; if ( $@ ) {
      my $e = $@;
      eval { local $SIG{'__DIE__'}; $self->unlock; };
      die $e;
    }

    return $self;
}

sub TIEHASH {
    shift;
    require DBM::Deep::Hash;
    return DBM::Deep::Hash->TIEHASH( @_ );
}

sub TIEARRAY {
    shift;
    require DBM::Deep::Array;
    return DBM::Deep::Array->TIEARRAY( @_ );
}

sub lock {
    my $self = shift->_get_self;
    return $self->_storage->lock( $self, @_ );
}

sub unlock {
    my $self = shift->_get_self;
    return $self->_storage->unlock( $self, @_ );
}

sub _copy_value {
    my $self = shift->_get_self;
    my ($spot, $value) = @_;

    if ( !ref $value ) {
        ${$spot} = $value;
    }
    elsif ( eval { local $SIG{__DIE__}; $value->isa( 'DBM::Deep' ) } ) {
        ${$spot} = $value->_repr;
        $value->_copy_node( ${$spot} );
    }
    else {
        my $r = Scalar::Util::reftype( $value );
        my $c = Scalar::Util::blessed( $value );
        if ( $r eq 'ARRAY' ) {
            ${$spot} = [ @{$value} ];
        }
        else {
            ${$spot} = { %{$value} };
        }
        ${$spot} = bless ${$spot}, $c
            if defined $c;
    }

    return 1;
}

#sub _copy_node {
#    die "Must be implemented in a child class\n";
#}
#
#sub _repr {
#    die "Must be implemented in a child class\n";
#}

sub export {
    ##
    # Recursively export into standard Perl hashes and arrays.
    ##
    my $self = shift->_get_self;

    my $temp = $self->_repr;

    $self->lock();
    $self->_copy_node( $temp );
    $self->unlock();

    my $classname = $self->_engine->get_classname( $self );
    if ( defined $classname ) {
      bless $temp, $classname;
    }

    return $temp;
}

sub import {
    ##
    # Recursively import Perl hash/array structure
    ##
    if (!ref($_[0])) { return; } # Perl calls import() on use -- ignore

    my $self = shift->_get_self;
    my ($struct) = @_;

    # struct is not a reference, so just import based on our type
    if (!ref($struct)) {
        $struct = $self->_repr( @_ );
    }

    #XXX This isn't the best solution. Better would be to use Data::Walker,
    #XXX but that's a lot more thinking than I want to do right now.
    eval {
        local $SIG{'__DIE__'};
        $self->begin_work;
        $self->_import( Clone::clone( $struct ) );
        $self->commit;
    }; if ( my $e = $@ ) {
        $self->rollback;
        die $e;
    }

    return 1;
}

#XXX Need to keep track of who has a fh to this file in order to
#XXX close them all prior to optimize on Win32/cygwin
sub optimize {
    ##
    # Rebuild entire database into new file, then move
    # it back on top of original.
    ##
    my $self = shift->_get_self;

#XXX Need to create a new test for this
#    if ($self->_storage->{links} > 1) {
#        $self->_throw_error("Cannot optimize: reference count is greater than 1");
#    }

    #XXX Do we have to lock the tempfile?

    my $db_temp = DBM::Deep->new(
        file => $self->_storage->{file} . '.tmp',
        type => $self->_type,

        # Bring over all the parameters that we need to bring over
        num_txns => $self->_engine->num_txns,
        byte_size => $self->_engine->byte_size,
        max_buckets => $self->_engine->max_buckets,
    );

    $self->lock();
    $self->_copy_node( $db_temp );
    undef $db_temp;

    ##
    # Attempt to copy user, group and permissions over to new file
    ##
    my @stats = stat($self->_fh);
    my $perms = $stats[2] & 07777;
    my $uid = $stats[4];
    my $gid = $stats[5];
    chown( $uid, $gid, $self->_storage->{file} . '.tmp' );
    chmod( $perms, $self->_storage->{file} . '.tmp' );

    # q.v. perlport for more information on this variable
    if ( $^O eq 'MSWin32' || $^O eq 'cygwin' ) {
        ##
        # Potential race condition when optmizing on Win32 with locking.
        # The Windows filesystem requires that the filehandle be closed
        # before it is overwritten with rename().  This could be redone
        # with a soft copy.
        ##
        $self->unlock();
        $self->_storage->close;
    }

    if (!rename $self->_storage->{file} . '.tmp', $self->_storage->{file}) {
        unlink $self->_storage->{file} . '.tmp';
        $self->unlock();
        $self->_throw_error("Optimize failed: Cannot copy temp file over original: $!");
    }

    $self->unlock();
    $self->_storage->close;

    $self->_storage->open;
    $self->lock();
    $self->_engine->setup_fh( $self );
    $self->unlock();

    return 1;
}

sub clone {
    ##
    # Make copy of object and return
    ##
    my $self = shift->_get_self;

    return DBM::Deep->new(
        type        => $self->_type,
        base_offset => $self->_base_offset,
        staleness   => $self->_staleness,
        storage     => $self->_storage,
        engine      => $self->_engine,
    );
}

#XXX Migrate this to the engine, where it really belongs and go through some
# API - stop poking in the innards of someone else..
{
    my %is_legal_filter = map {
        $_ => ~~1,
    } qw(
        store_key store_value
        fetch_key fetch_value
    );

    sub set_filter {
        ##
        # Setup filter function for storing or fetching the key or value
        ##
        my $self = shift->_get_self;
        my $type = lc shift;
        my $func = shift;

        if ( $is_legal_filter{$type} ) {
            $self->_storage->{"filter_$type"} = $func;
            return 1;
        }

        return;
    }
}

sub begin_work {
    my $self = shift->_get_self;
    return $self->_engine->begin_work( $self, @_ );
}

sub rollback {
    my $self = shift->_get_self;
    return $self->_engine->rollback( $self, @_ );
}

sub commit {
    my $self = shift->_get_self;
    return $self->_engine->commit( $self, @_ );
}

##
# Accessor methods
##

sub _engine {
    my $self = $_[0]->_get_self;
    return $self->{engine};
}

sub _storage {
    my $self = $_[0]->_get_self;
    return $self->{storage};
}

sub _type {
    my $self = $_[0]->_get_self;
    return $self->{type};
}

sub _base_offset {
    my $self = $_[0]->_get_self;
    return $self->{base_offset};
}

sub _staleness {
    my $self = $_[0]->_get_self;
    return $self->{staleness};
}

sub _fh {
    my $self = $_[0]->_get_self;
    return $self->_storage->{fh};
}

##
# Utility methods
##

sub _throw_error {
    die "DBM::Deep: $_[1]\n";
}

sub STORE {
    ##
    # Store single hash key/value or array element in database.
    ##
    my $self = shift->_get_self;
    my ($key, $value) = @_;

    if ( !FileHandle::Fmode::is_W( $self->_fh ) ) {
        $self->_throw_error( 'Cannot write to a readonly filehandle' );
    }

    ##
    # Request exclusive lock for writing
    ##
    $self->lock( LOCK_EX );

    # User may be storing a complex value, in which case we do not want it run
    # through the filtering system.
    if ( !ref($value) && $self->_storage->{filter_store_value} ) {
        $value = $self->_storage->{filter_store_value}->( $value );
    }

    $self->_engine->write_value( $self, $key, $value);

    $self->unlock();

    return 1;
}

sub FETCH {
    ##
    # Fetch single value or element given plain key or array index
    ##
    my $self = shift->_get_self;
    my ($key) = @_;

    ##
    # Request shared lock for reading
    ##
    $self->lock( LOCK_SH );

    my $result = $self->_engine->read_value( $self, $key);

    $self->unlock();

    # Filters only apply to scalar values, so the ref check is making
    # sure the fetched bucket is a scalar, not a child hash or array.
    return ($result && !ref($result) && $self->_storage->{filter_fetch_value})
        ? $self->_storage->{filter_fetch_value}->($result)
        : $result;
}

sub DELETE {
    ##
    # Delete single key/value pair or element given plain key or array index
    ##
    my $self = shift->_get_self;
    my ($key) = @_;

    if ( !FileHandle::Fmode::is_W( $self->_fh ) ) {
        $self->_throw_error( 'Cannot write to a readonly filehandle' );
    }

    ##
    # Request exclusive lock for writing
    ##
    $self->lock( LOCK_EX );

    ##
    # Delete bucket
    ##
    my $value = $self->_engine->delete_key( $self, $key);

    if (defined $value && !ref($value) && $self->_storage->{filter_fetch_value}) {
        $value = $self->_storage->{filter_fetch_value}->($value);
    }

    $self->unlock();

    return $value;
}

sub EXISTS {
    ##
    # Check if a single key or element exists given plain key or array index
    ##
    my $self = shift->_get_self;
    my ($key) = @_;

    ##
    # Request shared lock for reading
    ##
    $self->lock( LOCK_SH );

    my $result = $self->_engine->key_exists( $self, $key );

    $self->unlock();

    return $result;
}

sub CLEAR {
    ##
    # Clear all keys from hash, or all elements from array.
    ##
    my $self = shift->_get_self;

    if ( !FileHandle::Fmode::is_W( $self->_fh ) ) {
        $self->_throw_error( 'Cannot write to a readonly filehandle' );
    }

    ##
    # Request exclusive lock for writing
    ##
    $self->lock( LOCK_EX );

    #XXX Rewrite this dreck to do it in the engine as a tight loop vs.
    # iterating over keys - such a WASTE - is this required for transactional
    # clearning?! Surely that can be detected in the engine ...
    if ( $self->_type eq TYPE_HASH ) {
        my $key = $self->first_key;
        while ( $key ) {
            # Retrieve the key before deleting because we depend on next_key
            my $next_key = $self->next_key( $key );
            $self->_engine->delete_key( $self, $key, $key );
            $key = $next_key;
        }
    }
    else {
        my $size = $self->FETCHSIZE;
        for my $key ( 0 .. $size - 1 ) {
            $self->_engine->delete_key( $self, $key, $key );
        }
        $self->STORESIZE( 0 );
    }

    $self->unlock();

    return 1;
}

##
# Public method aliases
##
sub put { (shift)->STORE( @_ ) }
sub store { (shift)->STORE( @_ ) }
sub get { (shift)->FETCH( @_ ) }
sub fetch { (shift)->FETCH( @_ ) }
sub delete { (shift)->DELETE( @_ ) }
sub exists { (shift)->EXISTS( @_ ) }
sub clear { (shift)->CLEAR( @_ ) }

1;
__END__
