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

use 5.6.0;

use strict;
use warnings;

use Fcntl qw( :DEFAULT :flock :seek );
use Digest::MD5 ();
use Scalar::Util ();

use DBM::Deep::Engine;
use DBM::Deep::File;

use vars qw( $VERSION );
$VERSION = q(0.99_01);

##
# Setup constants for users to pass to new()
##
sub TYPE_HASH   () { DBM::Deep::Engine->SIG_HASH   }
sub TYPE_ARRAY  () { DBM::Deep::Engine->SIG_ARRAY  }

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

    $args->{fileobj} = DBM::Deep::File->new( $args )
        unless exists $args->{fileobj};

    # locking implicitly enables autoflush
    if ($args->{locking}) { $args->{autoflush} = 1; }

    # These are the defaults to be optionally overridden below
    my $self = bless {
        type        => TYPE_HASH,
        base_offset => undef,

        parent      => undef,
        parent_key  => undef,

        fileobj     => undef,
    }, $class;
    $self->{engine} = DBM::Deep::Engine->new( { %{$args}, obj => $self } );

    # Grab the parameters we want to use
    foreach my $param ( keys %$self ) {
        next unless exists $args->{$param};
        $self->{$param} = $args->{$param};
    }

    $self->{engine}->setup_fh( $self );

    $self->{fileobj}->set_db( $self );

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
    return $self->_fileobj->lock( $self, @_ );
}

sub unlock {
    my $self = shift->_get_self;
    return $self->_fileobj->unlock( $self, @_ );
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

sub _copy_node {
    die "Must be implemented in a child class\n";
}

sub _repr {
    die "Must be implemented in a child class\n";
}

sub export {
    ##
    # Recursively export into standard Perl hashes and arrays.
    ##
    my $self = shift->_get_self;

    my $temp = $self->_repr;

    $self->lock();
    $self->_copy_node( $temp );
    $self->unlock();

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

    return $self->_import( $struct );
}

sub optimize {
    ##
    # Rebuild entire database into new file, then move
    # it back on top of original.
    ##
    my $self = shift->_get_self;

#XXX Need to create a new test for this
#    if ($self->_fileobj->{links} > 1) {
#        $self->_throw_error("Cannot optimize: reference count is greater than 1");
#    }

    my $db_temp = DBM::Deep->new(
        file => $self->_fileobj->{file} . '.tmp',
        type => $self->_type
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
    chown( $uid, $gid, $self->_fileobj->{file} . '.tmp' );
    chmod( $perms, $self->_fileobj->{file} . '.tmp' );

    # q.v. perlport for more information on this variable
    if ( $^O eq 'MSWin32' || $^O eq 'cygwin' ) {
        ##
        # Potential race condition when optmizing on Win32 with locking.
        # The Windows filesystem requires that the filehandle be closed
        # before it is overwritten with rename().  This could be redone
        # with a soft copy.
        ##
        $self->unlock();
        $self->_fileobj->close;
    }

    if (!rename $self->_fileobj->{file} . '.tmp', $self->_fileobj->{file}) {
        unlink $self->_fileobj->{file} . '.tmp';
        $self->unlock();
        $self->_throw_error("Optimize failed: Cannot copy temp file over original: $!");
    }

    $self->unlock();
    $self->_fileobj->close;
    $self->_fileobj->open;
    $self->{engine}->setup_fh( $self );

    return 1;
}

sub clone {
    ##
    # Make copy of object and return
    ##
    my $self = shift->_get_self;

    return DBM::Deep->new(
        type => $self->_type,
        base_offset => $self->_base_offset,
        fileobj => $self->_fileobj,
    );
}

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
            $self->_fileobj->{"filter_$type"} = $func;
            return 1;
        }

        return;
    }
}

sub begin_work {
    my $self = shift->_get_self;
    $self->_fileobj->begin_transaction;
    return 1;
}

sub rollback {
    my $self = shift->_get_self;
    $self->_fileobj->end_transaction;
    return 1;
}

sub commit {
    my $self = shift->_get_self;
    # At this point, we need to replay the actions taken
    $self->_fileobj->end_transaction;
    return 1;
}

##
# Accessor methods
##

sub _fileobj {
    my $self = $_[0]->_get_self;
    return $self->{fileobj};
}

sub _type {
    my $self = $_[0]->_get_self;
    return $self->{type};
}

sub _base_offset {
    my $self = $_[0]->_get_self;
    return $self->{base_offset};
}

sub _fh {
    my $self = $_[0]->_get_self;
    return $self->_fileobj->{fh};
}

##
# Utility methods
##

sub _throw_error {
    die "DBM::Deep: $_[1]\n";
}

sub _is_writable {
    my $fh = shift;
    (O_WRONLY | O_RDWR) & fcntl( $fh, F_GETFL, my $slush = 0);
}

#sub _is_readable {
#    my $fh = shift;
#    (O_RDONLY | O_RDWR) & fcntl( $fh, F_GETFL, my $slush = 0);
#}

sub _find_parent {
    my $self = shift;
    if ( $self->{parent} ) {
        my $base = $self->{parent}->_find_parent();
        if ( $self->{parent}->_type eq TYPE_HASH ) {
            return $base . "\{$self->{parent_key}\}";
        }
        return $base . "\[$self->{parent_key}\]";
    }
    return '$db->';
}

sub STORE {
    ##
    # Store single hash key/value or array element in database.
    ##
    my $self = shift->_get_self;
    my ($key, $value, $orig_key) = @_;


    if ( $^O ne 'MSWin32' && !_is_writable( $self->_fh ) ) {
        $self->_throw_error( 'Cannot write to a readonly filehandle' );
    }

    if ( my $afh = $self->_fileobj->{audit_fh} ) {
        if ( defined $orig_key ) {
            my $lhs = $self->_find_parent;
            if ( $self->_type eq TYPE_HASH ) {
                $lhs .= "\{$orig_key\}";
            }
            else {
                $lhs .= "\[$orig_key\]";
            }

            my $rhs;

            my $r = Scalar::Util::reftype( $value ) || '';
            if ( $r eq 'HASH' ) {
                $rhs = '{}';
            }
            elsif ( $r eq 'ARRAY' ) {
                $rhs = '[]';
            }
            else {
                $rhs = "'$value'";
            }

            if ( my $c = Scalar::Util::blessed( $value ) ) {
                $rhs = "bless $rhs, '$c'";
            }

            $self->_fileobj->audit( "$lhs = $rhs;" );
#            flock( $afh, LOCK_EX );
#            print( $afh "$lhs = $rhs; # " . localtime(time) . "\n" );
#            flock( $afh, LOCK_UN );
        }
    }

    ##
    # Request exclusive lock for writing
    ##
    $self->lock( LOCK_EX );

    my $md5 = $self->{engine}{digest}->($key);

    my $tag = $self->{engine}->find_bucket_list( $self->_base_offset, $md5, { create => 1 } );

    # User may be storing a hash, in which case we do not want it run
    # through the filtering system
    if ( !ref($value) && $self->_fileobj->{filter_store_value} ) {
        $value = $self->_fileobj->{filter_store_value}->( $value );
    }

    ##
    # Add key/value to bucket list
    ##
    my $result = $self->{engine}->add_bucket( $tag, $md5, $key, $value, undef, $orig_key ); 

    $self->unlock();

    return $result;
}

sub FETCH {
    ##
    # Fetch single value or element given plain key or array index
    ##
    my $self = shift->_get_self;
    my ($key, $orig_key) = @_;

    my $md5 = $self->{engine}{digest}->($key);

    ##
    # Request shared lock for reading
    ##
    $self->lock( LOCK_SH );

    my $tag = $self->{engine}->find_bucket_list( $self->_base_offset, $md5 );
    if (!$tag) {
        $self->unlock();
        return;
    }

    ##
    # Get value from bucket list
    ##
    my $result = $self->{engine}->get_bucket_value( $tag, $md5, $orig_key );

    $self->unlock();

    # Filters only apply to scalar values, so the ref check is making
    # sure the fetched bucket is a scalar, not a child hash or array.
    return ($result && !ref($result) && $self->_fileobj->{filter_fetch_value})
        ? $self->_fileobj->{filter_fetch_value}->($result)
        : $result;
}

sub DELETE {
    ##
    # Delete single key/value pair or element given plain key or array index
    ##
    my $self = shift->_get_self;
    my ($key, $orig_key) = @_;

    if ( $^O ne 'MSWin32' && !_is_writable( $self->_fh ) ) {
        $self->_throw_error( 'Cannot write to a readonly filehandle' );
    }

    if ( my $afh = $self->_fileobj->{audit_fh} ) {
        if ( defined $orig_key ) {
            my $lhs = $self->_find_parent;
            if ( $self->_type eq TYPE_HASH ) {
                $lhs .= "\{$orig_key\}";
            }
            else {
                $lhs .= "\[$orig_key]\]";
            }

            flock( $afh, LOCK_EX );
            print( $afh "delete $lhs; # " . localtime(time) . "\n" );
            flock( $afh, LOCK_UN );
        }
    }

    ##
    # Request exclusive lock for writing
    ##
    $self->lock( LOCK_EX );

    my $md5 = $self->{engine}{digest}->($key);

    my $tag = $self->{engine}->find_bucket_list( $self->_base_offset, $md5 );
    if (!$tag) {
        $self->unlock();
        return;
    }

    ##
    # Delete bucket
    ##
    my $value = $self->{engine}->get_bucket_value( $tag, $md5 );

    if (defined $value && !ref($value) && $self->_fileobj->{filter_fetch_value}) {
        $value = $self->_fileobj->{filter_fetch_value}->($value);
    }

    my $result = $self->{engine}->delete_bucket( $tag, $md5, $orig_key );

    ##
    # If this object is an array and the key deleted was on the end of the stack,
    # decrement the length variable.
    ##

    $self->unlock();

    return $value;
}

sub EXISTS {
    ##
    # Check if a single key or element exists given plain key or array index
    ##
    my $self = shift->_get_self;
    my ($key) = @_;

    my $md5 = $self->{engine}{digest}->($key);

    ##
    # Request shared lock for reading
    ##
    $self->lock( LOCK_SH );

    my $tag = $self->{engine}->find_bucket_list( $self->_base_offset, $md5 );
    if (!$tag) {
        $self->unlock();

        ##
        # For some reason, the built-in exists() function returns '' for false
        ##
        return '';
    }

    ##
    # Check if bucket exists and return 1 or ''
    ##
    my $result = $self->{engine}->bucket_exists( $tag, $md5 ) || '';

    $self->unlock();

    return $result;
}

sub CLEAR {
    ##
    # Clear all keys from hash, or all elements from array.
    ##
    my $self = shift->_get_self;

    if ( $^O ne 'MSWin32' && !_is_writable( $self->_fh ) ) {
        $self->_throw_error( 'Cannot write to a readonly filehandle' );
    }

    if ( my $afh = $self->_fileobj->{audit_fh} ) {
        my $lhs = $self->_find_parent;

        my $rhs = '()';
        if ( $self->_type eq TYPE_HASH ) {
            $lhs = '%{' . $lhs . '}';
        }
        else {
            $lhs = '@{' . $lhs . '}';
        }

        flock( $afh, LOCK_EX );
        print( $afh "$lhs = $rhs; # " . localtime(time) . "\n" );
        flock( $afh, LOCK_UN );
    }

    ##
    # Request exclusive lock for writing
    ##
    $self->lock( LOCK_EX );

    my $fh = $self->_fh;

    seek($fh, $self->_base_offset + $self->_fileobj->{file_offset}, SEEK_SET);
    if (eof $fh) {
        $self->unlock();
        return;
    }

#XXX This needs updating to use _release_space
    $self->{engine}->write_tag(
        $self->_base_offset, $self->_type,
        chr(0)x$self->{engine}{index_size},
    );

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

=head1 NAME

DBM::Deep - A pure perl multi-level hash/array DBM

=head1 SYNOPSIS

  use DBM::Deep;
  my $db = DBM::Deep->new( "foo.db" );

  $db->{key} = 'value'; # tie() style
  print $db->{key};

  $db->put('key' => 'value'); # OO style
  print $db->get('key');

  # true multi-level support
  $db->{my_complex} = [
      'hello', { perl => 'rules' },
      42, 99,
  ];

=head1 DESCRIPTION

A unique flat-file database module, written in pure perl.  True
multi-level hash/array support (unlike MLDBM, which is faked), hybrid
OO / tie() interface, cross-platform FTPable files, and quite fast.  Can
handle millions of keys and unlimited hash levels without significant
slow-down.  Written from the ground-up in pure perl -- this is NOT a
wrapper around a C-based DBM.  Out-of-the-box compatibility with Unix,
Mac OS X and Windows.

=head1 VERSION DIFFERENCES

B<NOTE>: 0.99_01 and above have significant file format differences from 0.98 and
before. While attempts have been made to be backwards compatible, no guarantees.

=head1 INSTALLATION

Hopefully you are using Perl's excellent CPAN module, which will download
and install the module for you.  If not, get the tarball, and run these
commands:

    tar zxf DBM-Deep-*
    cd DBM-Deep-*
    perl Makefile.PL
    make
    make test
    make install

=head1 SETUP

Construction can be done OO-style (which is the recommended way), or using
Perl's tie() function.  Both are examined here.

=head2 OO CONSTRUCTION

The recommended way to construct a DBM::Deep object is to use the new()
method, which gets you a blessed, tied hash or array reference.

    my $db = DBM::Deep->new( "foo.db" );

This opens a new database handle, mapped to the file "foo.db".  If this
file does not exist, it will automatically be created.  DB files are
opened in "r+" (read/write) mode, and the type of object returned is a
hash, unless otherwise specified (see L<OPTIONS> below).

You can pass a number of options to the constructor to specify things like
locking, autoflush, etc.  This is done by passing an inline hash:

    my $db = DBM::Deep->new(
        file => "foo.db",
        locking => 1,
        autoflush => 1
    );

Notice that the filename is now specified I<inside> the hash with
the "file" parameter, as opposed to being the sole argument to the
constructor.  This is required if any options are specified.
See L<OPTIONS> below for the complete list.



You can also start with an array instead of a hash.  For this, you must
specify the C<type> parameter:

    my $db = DBM::Deep->new(
        file => "foo.db",
        type => DBM::Deep->TYPE_ARRAY
    );

B<Note:> Specifing the C<type> parameter only takes effect when beginning
a new DB file.  If you create a DBM::Deep object with an existing file, the
C<type> will be loaded from the file header, and an error will be thrown if
the wrong type is passed in.

=head2 TIE CONSTRUCTION

Alternately, you can create a DBM::Deep handle by using Perl's built-in
tie() function.  The object returned from tie() can be used to call methods,
such as lock() and unlock(), but cannot be used to assign to the DBM::Deep
file (as expected with most tie'd objects).

    my %hash;
    my $db = tie %hash, "DBM::Deep", "foo.db";

    my @array;
    my $db = tie @array, "DBM::Deep", "bar.db";

As with the OO constructor, you can replace the DB filename parameter with
a hash containing one or more options (see L<OPTIONS> just below for the
complete list).

    tie %hash, "DBM::Deep", {
        file => "foo.db",
        locking => 1,
        autoflush => 1
    };

=head2 OPTIONS

There are a number of options that can be passed in when constructing your
DBM::Deep objects.  These apply to both the OO- and tie- based approaches.

=over

=item * file

Filename of the DB file to link the handle to.  You can pass a full absolute
filesystem path, partial path, or a plain filename if the file is in the
current working directory.  This is a required parameter (though q.v. fh).

=item * fh

If you want, you can pass in the fh instead of the file. This is most useful for doing
something like:

  my $db = DBM::Deep->new( { fh => \*DATA } );

You are responsible for making sure that the fh has been opened appropriately for your
needs. If you open it read-only and attempt to write, an exception will be thrown. If you
open it write-only or append-only, an exception will be thrown immediately as DBM::Deep
needs to read from the fh.

=item * file_offset

This is the offset within the file that the DBM::Deep db starts. Most of the time, you will
not need to set this. However, it's there if you want it.

If you pass in fh and do not set this, it will be set appropriately.

=item * type

This parameter specifies what type of object to create, a hash or array.  Use
one of these two constants:

=over 4

=item * C<DBM::Deep-E<gt>TYPE_HASH>

=item * C<DBM::Deep-E<gt>TYPE_ARRAY>.

=back

This only takes effect when beginning a new file.  This is an optional
parameter, and defaults to C<DBM::Deep-E<gt>TYPE_HASH>.

=item * locking

Specifies whether locking is to be enabled.  DBM::Deep uses Perl's Fnctl flock()
function to lock the database in exclusive mode for writes, and shared mode for
reads.  Pass any true value to enable.  This affects the base DB handle I<and
any child hashes or arrays> that use the same DB file.  This is an optional
parameter, and defaults to 0 (disabled).  See L<LOCKING> below for more.

=item * autoflush

Specifies whether autoflush is to be enabled on the underlying filehandle.
This obviously slows down write operations, but is required if you may have
multiple processes accessing the same DB file (also consider enable I<locking>).
Pass any true value to enable.  This is an optional parameter, and defaults to 0
(disabled).

=item * autobless

If I<autobless> mode is enabled, DBM::Deep will preserve the class something
is blessed into, and restores it when fetched.  This is an optional parameter, and defaults to 1 (enabled).

B<Note:> If you use the OO-interface, you will not be able to call any methods
of DBM::Deep on the blessed item. This is considered to be a feature.

=item * filter_*

See L</FILTERS> below.

=back

=head1 TIE INTERFACE

With DBM::Deep you can access your databases using Perl's standard hash/array
syntax.  Because all DBM::Deep objects are I<tied> to hashes or arrays, you can
treat them as such.  DBM::Deep will intercept all reads/writes and direct them
to the right place -- the DB file.  This has nothing to do with the
L<TIE CONSTRUCTION> section above.  This simply tells you how to use DBM::Deep
using regular hashes and arrays, rather than calling functions like C<get()>
and C<put()> (although those work too).  It is entirely up to you how to want
to access your databases.

=head2 HASHES

You can treat any DBM::Deep object like a normal Perl hash reference.  Add keys,
or even nested hashes (or arrays) using standard Perl syntax:

    my $db = DBM::Deep->new( "foo.db" );

    $db->{mykey} = "myvalue";
    $db->{myhash} = {};
    $db->{myhash}->{subkey} = "subvalue";

    print $db->{myhash}->{subkey} . "\n";

You can even step through hash keys using the normal Perl C<keys()> function:

    foreach my $key (keys %$db) {
        print "$key: " . $db->{$key} . "\n";
    }

Remember that Perl's C<keys()> function extracts I<every> key from the hash and
pushes them onto an array, all before the loop even begins.  If you have an
extra large hash, this may exhaust Perl's memory.  Instead, consider using
Perl's C<each()> function, which pulls keys/values one at a time, using very
little memory:

    while (my ($key, $value) = each %$db) {
        print "$key: $value\n";
    }

Please note that when using C<each()>, you should always pass a direct
hash reference, not a lookup.  Meaning, you should B<never> do this:

    # NEVER DO THIS
    while (my ($key, $value) = each %{$db->{foo}}) { # BAD

This causes an infinite loop, because for each iteration, Perl is calling
FETCH() on the $db handle, resulting in a "new" hash for foo every time, so
it effectively keeps returning the first key over and over again. Instead,
assign a temporary variable to C<$db->{foo}>, then pass that to each().

=head2 ARRAYS

As with hashes, you can treat any DBM::Deep object like a normal Perl array
reference.  This includes inserting, removing and manipulating elements,
and the C<push()>, C<pop()>, C<shift()>, C<unshift()> and C<splice()> functions.
The object must have first been created using type C<DBM::Deep-E<gt>TYPE_ARRAY>,
or simply be a nested array reference inside a hash.  Example:

    my $db = DBM::Deep->new(
        file => "foo-array.db",
        type => DBM::Deep->TYPE_ARRAY
    );

    $db->[0] = "foo";
    push @$db, "bar", "baz";
    unshift @$db, "bah";

    my $last_elem = pop @$db; # baz
    my $first_elem = shift @$db; # bah
    my $second_elem = $db->[1]; # bar

    my $num_elements = scalar @$db;

=head1 OO INTERFACE

In addition to the I<tie()> interface, you can also use a standard OO interface
to manipulate all aspects of DBM::Deep databases.  Each type of object (hash or
array) has its own methods, but both types share the following common methods:
C<put()>, C<get()>, C<exists()>, C<delete()> and C<clear()>.

=over

=item * new() / clone()

These are the constructor and copy-functions.

=item * put() / store()

Stores a new hash key/value pair, or sets an array element value.  Takes two
arguments, the hash key or array index, and the new value.  The value can be
a scalar, hash ref or array ref.  Returns true on success, false on failure.

    $db->put("foo", "bar"); # for hashes
    $db->put(1, "bar"); # for arrays

=item * get() / fetch()

Fetches the value of a hash key or array element.  Takes one argument: the hash
key or array index.  Returns a scalar, hash ref or array ref, depending on the
data type stored.

    my $value = $db->get("foo"); # for hashes
    my $value = $db->get(1); # for arrays

=item * exists()

Checks if a hash key or array index exists.  Takes one argument: the hash key
or array index.  Returns true if it exists, false if not.

    if ($db->exists("foo")) { print "yay!\n"; } # for hashes
    if ($db->exists(1)) { print "yay!\n"; } # for arrays

=item * delete()

Deletes one hash key/value pair or array element.  Takes one argument: the hash
key or array index.  Returns true on success, false if not found.  For arrays,
the remaining elements located after the deleted element are NOT moved over.
The deleted element is essentially just undefined, which is exactly how Perl's
internal arrays work.  Please note that the space occupied by the deleted
key/value or element is B<not> reused again -- see L<UNUSED SPACE RECOVERY>
below for details and workarounds.

    $db->delete("foo"); # for hashes
    $db->delete(1); # for arrays

=item * clear()

Deletes B<all> hash keys or array elements.  Takes no arguments.  No return
value.  Please note that the space occupied by the deleted keys/values or
elements is B<not> reused again -- see L<UNUSED SPACE RECOVERY> below for
details and workarounds.

    $db->clear(); # hashes or arrays

=item * lock() / unlock()

q.v. Locking.

=item * optimize()

Recover lost disk space.

=item * import() / export()

Data going in and out.

=back

=head2 HASHES

For hashes, DBM::Deep supports all the common methods described above, and the
following additional methods: C<first_key()> and C<next_key()>.

=over

=item * first_key()

Returns the "first" key in the hash.  As with built-in Perl hashes, keys are
fetched in an undefined order (which appears random).  Takes no arguments,
returns the key as a scalar value.

    my $key = $db->first_key();

=item * next_key()

Returns the "next" key in the hash, given the previous one as the sole argument.
Returns undef if there are no more keys to be fetched.

    $key = $db->next_key($key);

=back

Here are some examples of using hashes:

    my $db = DBM::Deep->new( "foo.db" );

    $db->put("foo", "bar");
    print "foo: " . $db->get("foo") . "\n";

    $db->put("baz", {}); # new child hash ref
    $db->get("baz")->put("buz", "biz");
    print "buz: " . $db->get("baz")->get("buz") . "\n";

    my $key = $db->first_key();
    while ($key) {
        print "$key: " . $db->get($key) . "\n";
        $key = $db->next_key($key);
    }

    if ($db->exists("foo")) { $db->delete("foo"); }

=head2 ARRAYS

For arrays, DBM::Deep supports all the common methods described above, and the
following additional methods: C<length()>, C<push()>, C<pop()>, C<shift()>,
C<unshift()> and C<splice()>.

=over

=item * length()

Returns the number of elements in the array.  Takes no arguments.

    my $len = $db->length();

=item * push()

Adds one or more elements onto the end of the array.  Accepts scalars, hash
refs or array refs.  No return value.

    $db->push("foo", "bar", {});

=item * pop()

Fetches the last element in the array, and deletes it.  Takes no arguments.
Returns undef if array is empty.  Returns the element value.

    my $elem = $db->pop();

=item * shift()

Fetches the first element in the array, deletes it, then shifts all the
remaining elements over to take up the space.  Returns the element value.  This
method is not recommended with large arrays -- see L<LARGE ARRAYS> below for
details.

    my $elem = $db->shift();

=item * unshift()

Inserts one or more elements onto the beginning of the array, shifting all
existing elements over to make room.  Accepts scalars, hash refs or array refs.
No return value.  This method is not recommended with large arrays -- see
<LARGE ARRAYS> below for details.

    $db->unshift("foo", "bar", {});

=item * splice()

Performs exactly like Perl's built-in function of the same name.  See L<perldoc
-f splice> for usage -- it is too complicated to document here.  This method is
not recommended with large arrays -- see L<LARGE ARRAYS> below for details.

=back

Here are some examples of using arrays:

    my $db = DBM::Deep->new(
        file => "foo.db",
        type => DBM::Deep->TYPE_ARRAY
    );

    $db->push("bar", "baz");
    $db->unshift("foo");
    $db->put(3, "buz");

    my $len = $db->length();
    print "length: $len\n"; # 4

    for (my $k=0; $k<$len; $k++) {
        print "$k: " . $db->get($k) . "\n";
    }

    $db->splice(1, 2, "biz", "baf");

    while (my $elem = shift @$db) {
        print "shifted: $elem\n";
    }

=head1 LOCKING

Enable automatic file locking by passing a true value to the C<locking>
parameter when constructing your DBM::Deep object (see L<SETUP> above).

    my $db = DBM::Deep->new(
        file => "foo.db",
        locking => 1
    );

This causes DBM::Deep to C<flock()> the underlying filehandle with exclusive
mode for writes, and shared mode for reads.  This is required if you have
multiple processes accessing the same database file, to avoid file corruption.
Please note that C<flock()> does NOT work for files over NFS.  See L<DB OVER
NFS> below for more.

=head2 EXPLICIT LOCKING

You can explicitly lock a database, so it remains locked for multiple
transactions.  This is done by calling the C<lock()> method, and passing an
optional lock mode argument (defaults to exclusive mode).  This is particularly
useful for things like counters, where the current value needs to be fetched,
then incremented, then stored again.

    $db->lock();
    my $counter = $db->get("counter");
    $counter++;
    $db->put("counter", $counter);
    $db->unlock();

    # or...

    $db->lock();
    $db->{counter}++;
    $db->unlock();

You can pass C<lock()> an optional argument, which specifies which mode to use
(exclusive or shared).  Use one of these two constants: C<DBM::Deep-E<gt>LOCK_EX>
or C<DBM::Deep-E<gt>LOCK_SH>.  These are passed directly to C<flock()>, and are the
same as the constants defined in Perl's C<Fcntl> module.

    $db->lock( DBM::Deep->LOCK_SH );
    # something here
    $db->unlock();

=head1 IMPORTING/EXPORTING

You can import existing complex structures by calling the C<import()> method,
and export an entire database into an in-memory structure using the C<export()>
method.  Both are examined here.

=head2 IMPORTING

Say you have an existing hash with nested hashes/arrays inside it.  Instead of
walking the structure and adding keys/elements to the database as you go,
simply pass a reference to the C<import()> method.  This recursively adds
everything to an existing DBM::Deep object for you.  Here is an example:

    my $struct = {
        key1 => "value1",
        key2 => "value2",
        array1 => [ "elem0", "elem1", "elem2" ],
        hash1 => {
            subkey1 => "subvalue1",
            subkey2 => "subvalue2"
        }
    };

    my $db = DBM::Deep->new( "foo.db" );
    $db->import( $struct );

    print $db->{key1} . "\n"; # prints "value1"

This recursively imports the entire C<$struct> object into C<$db>, including
all nested hashes and arrays.  If the DBM::Deep object contains exsiting data,
keys are merged with the existing ones, replacing if they already exist.
The C<import()> method can be called on any database level (not just the base
level), and works with both hash and array DB types.

B<Note:> Make sure your existing structure has no circular references in it.
These will cause an infinite loop when importing.

=head2 EXPORTING

Calling the C<export()> method on an existing DBM::Deep object will return
a reference to a new in-memory copy of the database.  The export is done
recursively, so all nested hashes/arrays are all exported to standard Perl
objects.  Here is an example:

    my $db = DBM::Deep->new( "foo.db" );

    $db->{key1} = "value1";
    $db->{key2} = "value2";
    $db->{hash1} = {};
    $db->{hash1}->{subkey1} = "subvalue1";
    $db->{hash1}->{subkey2} = "subvalue2";

    my $struct = $db->export();

    print $struct->{key1} . "\n"; # prints "value1"

This makes a complete copy of the database in memory, and returns a reference
to it.  The C<export()> method can be called on any database level (not just
the base level), and works with both hash and array DB types.  Be careful of
large databases -- you can store a lot more data in a DBM::Deep object than an
in-memory Perl structure.

B<Note:> Make sure your database has no circular references in it.
These will cause an infinite loop when exporting.

=head1 FILTERS

DBM::Deep has a number of hooks where you can specify your own Perl function
to perform filtering on incoming or outgoing data.  This is a perfect
way to extend the engine, and implement things like real-time compression or
encryption.  Filtering applies to the base DB level, and all child hashes /
arrays.  Filter hooks can be specified when your DBM::Deep object is first
constructed, or by calling the C<set_filter()> method at any time.  There are
four available filter hooks, described below:

=over

=item * filter_store_key

This filter is called whenever a hash key is stored.  It
is passed the incoming key, and expected to return a transformed key.

=item * filter_store_value

This filter is called whenever a hash key or array element is stored.  It
is passed the incoming value, and expected to return a transformed value.

=item * filter_fetch_key

This filter is called whenever a hash key is fetched (i.e. via
C<first_key()> or C<next_key()>).  It is passed the transformed key,
and expected to return the plain key.

=item * filter_fetch_value

This filter is called whenever a hash key or array element is fetched.
It is passed the transformed value, and expected to return the plain value.

=back

Here are the two ways to setup a filter hook:

    my $db = DBM::Deep->new(
        file => "foo.db",
        filter_store_value => \&my_filter_store,
        filter_fetch_value => \&my_filter_fetch
    );

    # or...

    $db->set_filter( "filter_store_value", \&my_filter_store );
    $db->set_filter( "filter_fetch_value", \&my_filter_fetch );

Your filter function will be called only when dealing with SCALAR keys or
values.  When nested hashes and arrays are being stored/fetched, filtering
is bypassed.  Filters are called as static functions, passed a single SCALAR
argument, and expected to return a single SCALAR value.  If you want to
remove a filter, set the function reference to C<undef>:

    $db->set_filter( "filter_store_value", undef );

=head2 REAL-TIME ENCRYPTION EXAMPLE

Here is a working example that uses the I<Crypt::Blowfish> module to
do real-time encryption / decryption of keys & values with DBM::Deep Filters.
Please visit L<http://search.cpan.org/search?module=Crypt::Blowfish> for more
on I<Crypt::Blowfish>.  You'll also need the I<Crypt::CBC> module.

    use DBM::Deep;
    use Crypt::Blowfish;
    use Crypt::CBC;

    my $cipher = Crypt::CBC->new({
        'key'             => 'my secret key',
        'cipher'          => 'Blowfish',
        'iv'              => '$KJh#(}q',
        'regenerate_key'  => 0,
        'padding'         => 'space',
        'prepend_iv'      => 0
    });

    my $db = DBM::Deep->new(
        file => "foo-encrypt.db",
        filter_store_key => \&my_encrypt,
        filter_store_value => \&my_encrypt,
        filter_fetch_key => \&my_decrypt,
        filter_fetch_value => \&my_decrypt,
    );

    $db->{key1} = "value1";
    $db->{key2} = "value2";
    print "key1: " . $db->{key1} . "\n";
    print "key2: " . $db->{key2} . "\n";

    undef $db;
    exit;

    sub my_encrypt {
        return $cipher->encrypt( $_[0] );
    }
    sub my_decrypt {
        return $cipher->decrypt( $_[0] );
    }

=head2 REAL-TIME COMPRESSION EXAMPLE

Here is a working example that uses the I<Compress::Zlib> module to do real-time
compression / decompression of keys & values with DBM::Deep Filters.
Please visit L<http://search.cpan.org/search?module=Compress::Zlib> for
more on I<Compress::Zlib>.

    use DBM::Deep;
    use Compress::Zlib;

    my $db = DBM::Deep->new(
        file => "foo-compress.db",
        filter_store_key => \&my_compress,
        filter_store_value => \&my_compress,
        filter_fetch_key => \&my_decompress,
        filter_fetch_value => \&my_decompress,
    );

    $db->{key1} = "value1";
    $db->{key2} = "value2";
    print "key1: " . $db->{key1} . "\n";
    print "key2: " . $db->{key2} . "\n";

    undef $db;
    exit;

    sub my_compress {
        return Compress::Zlib::memGzip( $_[0] ) ;
    }
    sub my_decompress {
        return Compress::Zlib::memGunzip( $_[0] ) ;
    }

B<Note:> Filtering of keys only applies to hashes.  Array "keys" are
actually numerical index numbers, and are not filtered.

=head1 ERROR HANDLING

Most DBM::Deep methods return a true value for success, and call die() on
failure.  You can wrap calls in an eval block to catch the die.

    my $db = DBM::Deep->new( "foo.db" ); # create hash
    eval { $db->push("foo"); }; # ILLEGAL -- push is array-only call

    print $@;           # prints error message

=head1 LARGEFILE SUPPORT

If you have a 64-bit system, and your Perl is compiled with both LARGEFILE
and 64-bit support, you I<may> be able to create databases larger than 2 GB.
DBM::Deep by default uses 32-bit file offset tags, but these can be changed
by specifying the 'pack_size' parameter when constructing the file.

    DBM::Deep->new(
        filename => $filename,
        pack_size => 'large',
    );

This tells DBM::Deep to pack all file offsets with 8-byte (64-bit) quad words
instead of 32-bit longs.  After setting these values your DB files have a
theoretical maximum size of 16 XB (exabytes).

You can also use C<pack_size =E<gt> 'small'> in order to use 16-bit file
offsets.

B<Note:> Changing these values will B<NOT> work for existing database files.
Only change this for new files. Once the value has been set, it is stored in
the file's header and cannot be changed for the life of the file. These
parameters are per-file, meaning you can access 32-bit and 64-bit files, as
you chose.

B<Note:> We have not personally tested files larger than 2 GB -- all my
systems have only a 32-bit Perl.  However, I have received user reports that
this does indeed work!

=head1 LOW-LEVEL ACCESS

If you require low-level access to the underlying filehandle that DBM::Deep uses,
you can call the C<_fh()> method, which returns the handle:

    my $fh = $db->_fh();

This method can be called on the root level of the datbase, or any child
hashes or arrays.  All levels share a I<root> structure, which contains things
like the filehandle, a reference counter, and all the options specified
when you created the object.  You can get access to this file object by
calling the C<_fileobj()> method.

    my $file_obj = $db->_fileobj();

This is useful for changing options after the object has already been created,
such as enabling/disabling locking.  You can also store your own temporary user
data in this structure (be wary of name collision), which is then accessible from
any child hash or array.

=head1 CUSTOM DIGEST ALGORITHM

DBM::Deep by default uses the I<Message Digest 5> (MD5) algorithm for hashing
keys.  However you can override this, and use another algorithm (such as SHA-256)
or even write your own.  But please note that DBM::Deep currently expects zero
collisions, so your algorithm has to be I<perfect>, so to speak. Collision
detection may be introduced in a later version.

You can specify a custom digest algorithm by passing it into the parameter
list for new(), passing a reference to a subroutine as the 'digest' parameter,
and the length of the algorithm's hashes (in bytes) as the 'hash_size'
parameter. Here is a working example that uses a 256-bit hash from the
I<Digest::SHA256> module.  Please see
L<http://search.cpan.org/search?module=Digest::SHA256> for more information.

    use DBM::Deep;
    use Digest::SHA256;

    my $context = Digest::SHA256::new(256);

    my $db = DBM::Deep->new(
        filename => "foo-sha.db",
        digest => \&my_digest,
        hash_size => 32,
    );

    $db->{key1} = "value1";
    $db->{key2} = "value2";
    print "key1: " . $db->{key1} . "\n";
    print "key2: " . $db->{key2} . "\n";

    undef $db;
    exit;

    sub my_digest {
        return substr( $context->hash($_[0]), 0, 32 );
    }

B<Note:> Your returned digest strings must be B<EXACTLY> the number
of bytes you specify in the hash_size parameter (in this case 32).

B<Note:> If you do choose to use a custom digest algorithm, you must set it
every time you access this file. Otherwise, the default (MD5) will be used.

=head1 CIRCULAR REFERENCES

DBM::Deep has B<experimental> support for circular references.  Meaning you
can have a nested hash key or array element that points to a parent object.
This relationship is stored in the DB file, and is preserved between sessions.
Here is an example:

    my $db = DBM::Deep->new( "foo.db" );

    $db->{foo} = "bar";
    $db->{circle} = $db; # ref to self

    print $db->{foo} . "\n"; # prints "bar"
    print $db->{circle}->{foo} . "\n"; # prints "bar" again

B<Note>: Passing the object to a function that recursively walks the
object tree (such as I<Data::Dumper> or even the built-in C<optimize()> or
C<export()> methods) will result in an infinite loop. This will be fixed in
a future release.

=head1 CAVEATS / ISSUES / BUGS

This section describes all the known issues with DBM::Deep.  It you have found
something that is not listed here, please send e-mail to L<jhuckaby@cpan.org>.

=head2 UNUSED SPACE RECOVERY

One major caveat with DBM::Deep is that space occupied by existing keys and
values is not recovered when they are deleted.  Meaning if you keep deleting
and adding new keys, your file will continuously grow.  I am working on this,
but in the meantime you can call the built-in C<optimize()> method from time to
time (perhaps in a crontab or something) to recover all your unused space.

    $db->optimize(); # returns true on success

This rebuilds the ENTIRE database into a new file, then moves it on top of
the original.  The new file will have no unused space, thus it will take up as
little disk space as possible.  Please note that this operation can take
a long time for large files, and you need enough disk space to temporarily hold
2 copies of your DB file.  The temporary file is created in the same directory
as the original, named with a ".tmp" extension, and is deleted when the
operation completes.  Oh, and if locking is enabled, the DB is automatically
locked for the entire duration of the copy.

B<WARNING:> Only call optimize() on the top-level node of the database, and
make sure there are no child references lying around.  DBM::Deep keeps a reference
counter, and if it is greater than 1, optimize() will abort and return undef.

=head2 REFERENCES

(The reasons given assume a high level of Perl understanding, specifically of
references. You can safely skip this section.)

Currently, the only references supported are HASH and ARRAY. The other reference
types (SCALAR, CODE, GLOB, and REF) cannot be supported for various reasons.

=over 4

=item * GLOB

These are things like filehandles and other sockets. They can't be supported
because it's completely unclear how DBM::Deep should serialize them.

=item * SCALAR / REF

The discussion here refers to the following type of example:

  my $x = 25;
  $db->{key1} = \$x;

  $x = 50;

  # In some other process ...

  my $val = ${ $db->{key1} };

  is( $val, 50, "What actually gets stored in the DB file?" );

The problem is one of synchronization. When the variable being referred to
changes value, the reference isn't notified. This means that the new value won't
be stored in the datafile for other processes to read. There is no TIEREF.

It is theoretically possible to store references to values already within a
DBM::Deep object because everything already is synchronized, but the change to
the internals would be quite large. Specifically, DBM::Deep would have to tie
every single value that is stored. This would bloat the RAM footprint of
DBM::Deep at least twofold (if not more) and be a significant performance drain,
all to support a feature that has never been requested.

=item * CODE

L<http://search.cpan.org/search?module=Data::Dump::Streamer> provides a
mechanism for serializing coderefs, including saving off all closure state.
However, just as for SCALAR and REF, that closure state may change without
notifying the DBM::Deep object storing the reference.

=back

=head2 FILE CORRUPTION

The current level of error handling in DBM::Deep is minimal.  Files I<are> checked
for a 32-bit signature when opened, but other corruption in files can cause
segmentation faults.  DBM::Deep may try to seek() past the end of a file, or get
stuck in an infinite loop depending on the level of corruption.  File write
operations are not checked for failure (for speed), so if you happen to run
out of disk space, DBM::Deep will probably fail in a bad way.  These things will
be addressed in a later version of DBM::Deep.

=head2 DB OVER NFS

Beware of using DB files over NFS.  DBM::Deep uses flock(), which works well on local
filesystems, but will NOT protect you from file corruption over NFS.  I've heard
about setting up your NFS server with a locking daemon, then using lockf() to
lock your files, but your mileage may vary there as well.  From what I
understand, there is no real way to do it.  However, if you need access to the
underlying filehandle in DBM::Deep for using some other kind of locking scheme like
lockf(), see the L<LOW-LEVEL ACCESS> section above.

=head2 COPYING OBJECTS

Beware of copying tied objects in Perl.  Very strange things can happen.
Instead, use DBM::Deep's C<clone()> method which safely copies the object and
returns a new, blessed, tied hash or array to the same level in the DB.

    my $copy = $db->clone();

B<Note>: Since clone() here is cloning the object, not the database location, any
modifications to either $db or $copy will be visible in both.

=head2 LARGE ARRAYS

Beware of using C<shift()>, C<unshift()> or C<splice()> with large arrays.
These functions cause every element in the array to move, which can be murder
on DBM::Deep, as every element has to be fetched from disk, then stored again in
a different location.  This will be addressed in the forthcoming version 1.00.

=head2 WRITEONLY FILES

If you pass in a filehandle to new(), you may have opened it in either a readonly or
writeonly mode. STORE will verify that the filehandle is writable. However, there
doesn't seem to be a good way to determine if a filehandle is readable. And, if the
filehandle isn't readable, it's not clear what will happen. So, don't do that.

=head1 PERFORMANCE

This section discusses DBM::Deep's speed and memory usage.

=head2 SPEED

Obviously, DBM::Deep isn't going to be as fast as some C-based DBMs, such as
the almighty I<BerkeleyDB>.  But it makes up for it in features like true
multi-level hash/array support, and cross-platform FTPable files.  Even so,
DBM::Deep is still pretty fast, and the speed stays fairly consistent, even
with huge databases.  Here is some test data:

    Adding 1,000,000 keys to new DB file...

    At 100 keys, avg. speed is 2,703 keys/sec
    At 200 keys, avg. speed is 2,642 keys/sec
    At 300 keys, avg. speed is 2,598 keys/sec
    At 400 keys, avg. speed is 2,578 keys/sec
    At 500 keys, avg. speed is 2,722 keys/sec
    At 600 keys, avg. speed is 2,628 keys/sec
    At 700 keys, avg. speed is 2,700 keys/sec
    At 800 keys, avg. speed is 2,607 keys/sec
    At 900 keys, avg. speed is 2,190 keys/sec
    At 1,000 keys, avg. speed is 2,570 keys/sec
    At 2,000 keys, avg. speed is 2,417 keys/sec
    At 3,000 keys, avg. speed is 1,982 keys/sec
    At 4,000 keys, avg. speed is 1,568 keys/sec
    At 5,000 keys, avg. speed is 1,533 keys/sec
    At 6,000 keys, avg. speed is 1,787 keys/sec
    At 7,000 keys, avg. speed is 1,977 keys/sec
    At 8,000 keys, avg. speed is 2,028 keys/sec
    At 9,000 keys, avg. speed is 2,077 keys/sec
    At 10,000 keys, avg. speed is 2,031 keys/sec
    At 20,000 keys, avg. speed is 1,970 keys/sec
    At 30,000 keys, avg. speed is 2,050 keys/sec
    At 40,000 keys, avg. speed is 2,073 keys/sec
    At 50,000 keys, avg. speed is 1,973 keys/sec
    At 60,000 keys, avg. speed is 1,914 keys/sec
    At 70,000 keys, avg. speed is 2,091 keys/sec
    At 80,000 keys, avg. speed is 2,103 keys/sec
    At 90,000 keys, avg. speed is 1,886 keys/sec
    At 100,000 keys, avg. speed is 1,970 keys/sec
    At 200,000 keys, avg. speed is 2,053 keys/sec
    At 300,000 keys, avg. speed is 1,697 keys/sec
    At 400,000 keys, avg. speed is 1,838 keys/sec
    At 500,000 keys, avg. speed is 1,941 keys/sec
    At 600,000 keys, avg. speed is 1,930 keys/sec
    At 700,000 keys, avg. speed is 1,735 keys/sec
    At 800,000 keys, avg. speed is 1,795 keys/sec
    At 900,000 keys, avg. speed is 1,221 keys/sec
    At 1,000,000 keys, avg. speed is 1,077 keys/sec

This test was performed on a PowerMac G4 1gHz running Mac OS X 10.3.2 & Perl
5.8.1, with an 80GB Ultra ATA/100 HD spinning at 7200RPM.  The hash keys and
values were between 6 - 12 chars in length.  The DB file ended up at 210MB.
Run time was 12 min 3 sec.

=head2 MEMORY USAGE

One of the great things about DBM::Deep is that it uses very little memory.
Even with huge databases (1,000,000+ keys) you will not see much increased
memory on your process.  DBM::Deep relies solely on the filesystem for storing
and fetching data.  Here is output from I</usr/bin/top> before even opening a
database handle:

      PID USER     PRI  NI  SIZE  RSS SHARE STAT %CPU %MEM   TIME COMMAND
    22831 root      11   0  2716 2716  1296 R     0.0  0.2   0:07 perl

Basically the process is taking 2,716K of memory.  And here is the same
process after storing and fetching 1,000,000 keys:

      PID USER     PRI  NI  SIZE  RSS SHARE STAT %CPU %MEM   TIME COMMAND
    22831 root      14   0  2772 2772  1328 R     0.0  0.2  13:32 perl

Notice the memory usage increased by only 56K.  Test was performed on a 700mHz
x86 box running Linux RedHat 7.2 & Perl 5.6.1.

=head1 DB FILE FORMAT

In case you were interested in the underlying DB file format, it is documented
here in this section.  You don't need to know this to use the module, it's just
included for reference.

=head2 SIGNATURE

DBM::Deep files always start with a 32-bit signature to identify the file type.
This is at offset 0.  The signature is "DPDB" in network byte order.  This is
checked for when the file is opened and an error will be thrown if it's not found.

=head2 TAG

The DBM::Deep file is in a I<tagged format>, meaning each section of the file
has a standard header containing the type of data, the length of data, and then
the data itself.  The type is a single character (1 byte), the length is a
32-bit unsigned long in network byte order, and the data is, well, the data.
Here is how it unfolds:

=head2 MASTER INDEX

Immediately after the 32-bit file signature is the I<Master Index> record.
This is a standard tag header followed by 1024 bytes (in 32-bit mode) or 2048
bytes (in 64-bit mode) of data.  The type is I<H> for hash or I<A> for array,
depending on how the DBM::Deep object was constructed.

The index works by looking at a I<MD5 Hash> of the hash key (or array index
number).  The first 8-bit char of the MD5 signature is the offset into the
index, multipled by 4 in 32-bit mode, or 8 in 64-bit mode.  The value of the
index element is a file offset of the next tag for the key/element in question,
which is usually a I<Bucket List> tag (see below).

The next tag I<could> be another index, depending on how many keys/elements
exist.  See L<RE-INDEXING> below for details.

=head2 BUCKET LIST

A I<Bucket List> is a collection of 16 MD5 hashes for keys/elements, plus
file offsets to where the actual data is stored.  It starts with a standard
tag header, with type I<B>, and a data size of 320 bytes in 32-bit mode, or
384 bytes in 64-bit mode.  Each MD5 hash is stored in full (16 bytes), plus
the 32-bit or 64-bit file offset for the I<Bucket> containing the actual data.
When the list fills up, a I<Re-Index> operation is performed (See
L<RE-INDEXING> below).

=head2 BUCKET

A I<Bucket> is a tag containing a key/value pair (in hash mode), or a
index/value pair (in array mode).  It starts with a standard tag header with
type I<D> for scalar data (string, binary, etc.), or it could be a nested
hash (type I<H>) or array (type I<A>).  The value comes just after the tag
header.  The size reported in the tag header is only for the value, but then,
just after the value is another size (32-bit unsigned long) and then the plain
key itself.  Since the value is likely to be fetched more often than the plain
key, I figured it would be I<slightly> faster to store the value first.

If the type is I<H> (hash) or I<A> (array), the value is another I<Master Index>
record for the nested structure, where the process begins all over again.

=head2 RE-INDEXING

After a I<Bucket List> grows to 16 records, its allocated space in the file is
exhausted.  Then, when another key/element comes in, the list is converted to a
new index record.  However, this index will look at the next char in the MD5
hash, and arrange new Bucket List pointers accordingly.  This process is called
I<Re-Indexing>.  Basically, a new index tag is created at the file EOF, and all
17 (16 + new one) keys/elements are removed from the old Bucket List and
inserted into the new index.  Several new Bucket Lists are created in the
process, as a new MD5 char from the key is being examined (it is unlikely that
the keys will all share the same next char of their MD5s).

Because of the way the I<MD5> algorithm works, it is impossible to tell exactly
when the Bucket Lists will turn into indexes, but the first round tends to
happen right around 4,000 keys.  You will see a I<slight> decrease in
performance here, but it picks back up pretty quick (see L<SPEED> above).  Then
it takes B<a lot> more keys to exhaust the next level of Bucket Lists.  It's
right around 900,000 keys.  This process can continue nearly indefinitely --
right up until the point the I<MD5> signatures start colliding with each other,
and this is B<EXTREMELY> rare -- like winning the lottery 5 times in a row AND
getting struck by lightning while you are walking to cash in your tickets.
Theoretically, since I<MD5> hashes are 128-bit values, you I<could> have up to
340,282,366,921,000,000,000,000,000,000,000,000,000 keys/elements (I believe
this is 340 unodecillion, but don't quote me).

=head2 STORING

When a new key/element is stored, the key (or index number) is first run through
I<Digest::MD5> to get a 128-bit signature (example, in hex:
b05783b0773d894396d475ced9d2f4f6).  Then, the I<Master Index> record is checked
for the first char of the signature (in this case I<b0>).  If it does not exist,
a new I<Bucket List> is created for our key (and the next 15 future keys that
happen to also have I<b> as their first MD5 char).  The entire MD5 is written
to the I<Bucket List> along with the offset of the new I<Bucket> record (EOF at
this point, unless we are replacing an existing I<Bucket>), where the actual
data will be stored.

=head2 FETCHING

Fetching an existing key/element involves getting a I<Digest::MD5> of the key
(or index number), then walking along the indexes.  If there are enough
keys/elements in this DB level, there might be nested indexes, each linked to
a particular char of the MD5.  Finally, a I<Bucket List> is pointed to, which
contains up to 16 full MD5 hashes.  Each is checked for equality to the key in
question.  If we found a match, the I<Bucket> tag is loaded, where the value and
plain key are stored.

Fetching the plain key occurs when calling the I<first_key()> and I<next_key()>
methods.  In this process the indexes are walked systematically, and each key
fetched in increasing MD5 order (which is why it appears random).   Once the
I<Bucket> is found, the value is skipped and the plain key returned instead.
B<Note:> Do not count on keys being fetched as if the MD5 hashes were
alphabetically sorted.  This only happens on an index-level -- as soon as the
I<Bucket Lists> are hit, the keys will come out in the order they went in --
so it's pretty much undefined how the keys will come out -- just like Perl's
built-in hashes.

=head1 CODE COVERAGE

We use B<Devel::Cover> to test the code coverage of our tests, below is the
B<Devel::Cover> report on this module's test suite.

  ----------------------------------- ------ ------ ------ ------ ------ ------
  File                                  stmt   bran   cond    sub   time  total
  ----------------------------------- ------ ------ ------ ------ ------ ------
  blib/lib/DBM/Deep.pm                  94.9   80.6   73.0  100.0   37.9   90.4
  blib/lib/DBM/Deep/Array.pm           100.0   91.1  100.0  100.0   18.2   98.1
  blib/lib/DBM/Deep/Engine.pm           98.9   87.3   80.0  100.0   34.2   95.2
  blib/lib/DBM/Deep/Hash.pm            100.0   87.5  100.0  100.0    9.7   97.3
  Total                                 97.9   85.9   79.7  100.0  100.0   94.3
  ----------------------------------- ------ ------ ------ ------ ------ ------

=head1 MORE INFORMATION

Check out the DBM::Deep Google Group at L<http://groups.google.com/group/DBM-Deep>
or send email to L<DBM-Deep@googlegroups.com>.

=head1 AUTHORS

Joseph Huckaby, L<jhuckaby@cpan.org>

Rob Kinyon, L<rkinyon@cpan.org>

Special thanks to Adam Sah and Rich Gaushell!  You know why :-)

=head1 SEE ALSO

perltie(1), Tie::Hash(3), Digest::MD5(3), Fcntl(3), flock(2), lockf(3), nfs(5),
Digest::SHA256(3), Crypt::Blowfish(3), Compress::Zlib(3)

=head1 LICENSE

Copyright (c) 2002-2006 Joseph Huckaby.  All Rights Reserved.
This is free software, you may use it and distribute it under the
same terms as Perl itself.

=cut
