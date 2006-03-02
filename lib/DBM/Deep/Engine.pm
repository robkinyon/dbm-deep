package DBM::Deep::Engine;
use XXX;

use strict;

use Fcntl qw( :DEFAULT :flock :seek );

sub precalc_sizes {
    ##
    # Precalculate index, bucket and bucket list sizes
    ##
    my $self = shift;

    $self->{index_size}       = (2**8) * $self->{long_size};
    $self->{bucket_size}      = $self->{hash_size} + $self->{long_size};
    $self->{bucket_list_size} = $self->{max_buckets} * $self->{bucket_size};

    return 1;
}

sub set_pack {
    ##
    # Set pack/unpack modes (see file header for more)
    ##
    my $self = shift;
    my ($long_s, $long_p, $data_s, $data_p) = @_;

    ##
    # Set to 4 and 'N' for 32-bit offset tags (default).  Theoretical limit of 4 GB per file.
    #    (Perl must be compiled with largefile support for files > 2 GB)
    #
    # Set to 8 and 'Q' for 64-bit offsets.  Theoretical limit of 16 XB per file.
    #    (Perl must be compiled with largefile and 64-bit long support)
    ##
    $self->{long_size} = $long_s ? $long_s : 4;
    $self->{long_pack} = $long_p ? $long_p : 'N';

    ##
    # Set to 4 and 'N' for 32-bit data length prefixes.  Limit of 4 GB for each key/value.
    # Upgrading this is possible (see above) but probably not necessary.  If you need
    # more than 4 GB for a single key or value, this module is really not for you :-)
    ##
    $self->{data_size} = $data_s ? $data_s : 4;
    $self->{data_pack} = $data_p ? $data_p : 'N';

    return $self->precalc_sizes();
}

sub set_digest {
    ##
    # Set key digest function (default is MD5)
    ##
    my $self = shift;
    my ($digest_func, $hash_size) = @_;

    $self->{digest} = $digest_func ? $digest_func : \&Digest::MD5::md5;
    $self->{hash_size} = $hash_size ? $hash_size : 16;

    return $self->precalc_sizes();
}

sub new {
    my $class = shift;
    my ($args) = @_;

    my $self = bless {
        long_size   => 4,
        long_pack   => 'N',
        data_size   => 4,
        data_pack   => 'N',

        digest      => \&Digest::MD5::md5,
        hash_size   => 16,

        ##
        # Maximum number of buckets per list before another level of indexing is done.
        # Increase this value for slightly greater speed, but larger database files.
        # DO NOT decrease this value below 16, due to risk of recursive reindex overrun.
        ##
        max_buckets => 16,
    }, $class;

    $self->precalc_sizes;

    return $self;
}

sub setup_fh {
    my $self = shift;
    my ($obj) = @_;

    $self->open( $obj ) if !defined $obj->_fh;

    #XXX We have to make sure we don't mess up when autoflush isn't turned on
    unless ( $obj->_root->{inode} ) {
        my @stats = stat($obj->_fh);
        $obj->_root->{inode} = $stats[1];
        $obj->_root->{end} = $stats[7];
    }

    return 1;
}

sub open {
    ##
    # Open a fh to the database, create if nonexistent.
    # Make sure file signature matches DBM::Deep spec.
    ##
    my $self = shift;
    my ($obj) = @_;

    if (defined($obj->_fh)) { $self->close_fh( $obj ); }

    # Theoretically, adding O_BINARY should remove the need for the binmode
    # Of course, testing it is going to be ... interesting.
    my $flags = O_RDWR | O_CREAT | O_BINARY;

    my $fh;
    sysopen( $fh, $obj->_root->{file}, $flags )
        or $obj->_throw_error("Cannot sysopen file: " . $obj->_root->{file} . ": $!");
    $obj->_root->{fh} = $fh;

    #XXX Can we remove this by using the right sysopen() flags?
    # Maybe ... q.v. above
    binmode $fh; # for win32

    if ($obj->_root->{autoflush}) {
        my $old = select $fh;
        $|=1;
        select $old;
    }

    seek($fh, 0 + $obj->_root->{file_offset}, SEEK_SET);

    my $signature;
    my $bytes_read = read( $fh, $signature, length(DBM::Deep->SIG_FILE));

    ##
    # File is empty -- write signature and master index
    ##
    if (!$bytes_read) {
        seek($fh, 0 + $obj->_root->{file_offset}, SEEK_SET);
        print( $fh DBM::Deep->SIG_FILE);

        $self->create_tag($obj, $obj->_base_offset, $obj->_type, chr(0) x $self->{index_size});

        # Why is this being printed here? I'm not seeing where anything actually points to
        # this spot.
        #XXX $obj->_root->{end} isn't updated from these 10 bytes that are being written
        my $plain_key = "[base]";
        print( $fh pack($self->{data_pack}, length($plain_key)) . $plain_key );

        # Flush the filehandle
        my $old_fh = select $fh;
        my $old_af = $|; $| = 1; $| = $old_af;
        select $old_fh;

        return 1;
    }

    ##
    # Check signature was valid
    ##
    unless ($signature eq DBM::Deep->SIG_FILE) {
        $self->close_fh( $obj );
        $obj->_throw_error("Signature not found -- file is not a Deep DB");
    }

    ##
    # Get our type from master index signature
    ##
    my $tag = $self->load_tag($obj, $obj->_base_offset)
        or $obj->_throw_error("Corrupted file, no master index record");

    unless ($obj->{type} eq $tag->{signature}) {
        $obj->_throw_error("File type mismatch");
    }

#XXX We probably also want to store the hash algorithm name and not assume anything
#XXX The cool thing would be to allow a different hashing algorithm at every level

    return 1;
}

sub close_fh {
    my $self = shift;
    my ($obj) = @_;

    if ( my $fh = $obj->_root->{fh} ) {
        close $fh;
    }
    $obj->_root->{fh} = undef;

    return 1;
}

sub create_tag {
    ##
    # Given offset, signature and content, create tag and write to disk
    ##
    my $self = shift;
    my ($obj, $offset, $sig, $content) = @_;
    my $size = length($content);

    my $fh = $obj->_fh;

    seek($fh, $offset + $obj->_root->{file_offset}, SEEK_SET);
    print( $fh $sig . pack($self->{data_pack}, $size) . $content );

    if ($offset == $obj->_root->{end}) {
        $obj->_root->{end} += DBM::Deep->SIG_SIZE + $self->{data_size} + $size;
    }

    return {
        signature => $sig,
        size => $size,
        offset => $offset + DBM::Deep->SIG_SIZE + $self->{data_size},
        content => $content
    };
}

sub load_tag {
    ##
    # Given offset, load single tag and return signature, size and data
    ##
    my $self = shift;
    my ($obj, $offset) = @_;

    my $fh = $obj->_fh;

    seek($fh, $offset + $obj->_root->{file_offset}, SEEK_SET);

    #XXX I'm not sure this check will work given autoflush ...
    return if eof $fh;

    my $b;
    read( $fh, $b, DBM::Deep->SIG_SIZE + $self->{data_size} );
    my ($sig, $size) = unpack( "A $self->{data_pack}", $b );

    my $buffer;
    read( $fh, $buffer, $size);

    return {
        signature => $sig,
        size => $size,
        offset => $offset + DBM::Deep->SIG_SIZE + $self->{data_size},
        content => $buffer
    };
}

sub add_bucket {
    ##
    # Adds one key/value pair to bucket list, given offset, MD5 digest of key,
    # plain (undigested) key and value.
    ##
    my $self = shift;
    my ($obj, $tag, $md5, $plain_key, $value) = @_;
    my $keys = $tag->{content};
    my $location = 0;
    my $result = 2;

    my $root = $obj->_root;

    my $is_dbm_deep = eval { local $SIG{'__DIE__'}; $value->isa( 'DBM::Deep' ) };
    my $internal_ref = $is_dbm_deep && ($value->_root eq $root);

    my $fh = $obj->_fh;

    ##
    # Iterate through buckets, seeing if this is a new entry or a replace.
    ##
    for (my $i = 0; $i < $self->{max_buckets}; $i++) {
        my $subloc = unpack($self->{long_pack}, substr($keys, ($i * $self->{bucket_size}) + $self->{hash_size}, $self->{long_size}));
        if (!$subloc) {
            ##
            # Found empty bucket (end of list).  Populate and exit loop.
            ##
            $result = 2;

            $location = $internal_ref
                ? $value->_base_offset
                : $root->{end};
print "NEW: $location\n";

            seek($fh, $tag->{offset} + ($i * $self->{bucket_size}) + $root->{file_offset}, SEEK_SET);
            print( $fh $md5 . pack($self->{long_pack}, $location) );
            last;
        }

        my $key = substr($keys, $i * $self->{bucket_size}, $self->{hash_size});
        if ($md5 eq $key) {
            ##
            # Found existing bucket with same key.  Replace with new value.
            ##
            $result = 1;

            if ($internal_ref) {
                $location = $value->_base_offset;
                seek($fh, $tag->{offset} + ($i * $self->{bucket_size}) + $root->{file_offset}, SEEK_SET);
                print( $fh $md5 . pack($self->{long_pack}, $location) );
                return $result;
            }

            seek($fh, $subloc + DBM::Deep->SIG_SIZE + $root->{file_offset}, SEEK_SET);
            my $size;
            read( $fh, $size, $self->{data_size}); $size = unpack($self->{data_pack}, $size);

            ##
            # If value is a hash, array, or raw value with equal or less size, we can
            # reuse the same content area of the database.  Otherwise, we have to create
            # a new content area at the EOF.
            ##
            my $actual_length;
            my $r = Scalar::Util::reftype( $value ) || '';
            if ( $r eq 'HASH' || $r eq 'ARRAY' ) {
                $actual_length = $self->{index_size};

                # if autobless is enabled, must also take into consideration
                # the class name, as it is stored along with key/value.
                if ( $root->{autobless} ) {
                    my $value_class = Scalar::Util::blessed($value);
                    if ( defined $value_class && !$value->isa('DBM::Deep') ) {
                        $actual_length += length($value_class);
                    }
                }
            }
            else { $actual_length = length($value); }

            if ($actual_length <= $size) {
                $location = $subloc;
            }
            else {
                $location = $root->{end};
                seek($fh, $tag->{offset} + ($i * $self->{bucket_size}) + $self->{hash_size} + $root->{file_offset}, SEEK_SET);
                print( $fh pack($self->{long_pack}, $location) );
            }

            last;
        }
    }

    ##
    # If this is an internal reference, return now.
    # No need to write value or plain key
    ##
    if ($internal_ref) {
        return $result;
    }

    ##
    # If bucket didn't fit into list, split into a new index level
    ##
    if (!$location) {
        seek($fh, $tag->{ref_loc} + $root->{file_offset}, SEEK_SET);
        print( $fh pack($self->{long_pack}, $root->{end}) );

        my $index_tag = $self->create_tag($obj, $root->{end}, DBM::Deep->SIG_INDEX, chr(0) x $self->{index_size});
        my @offsets = ();

        $keys .= $md5 . pack($self->{long_pack}, 0);

        for (my $i=0; $i<=$self->{max_buckets}; $i++) {
            my $key = substr($keys, $i * $self->{bucket_size}, $self->{hash_size});
            if ($key) {
                my $old_subloc = unpack($self->{long_pack}, substr($keys, ($i * $self->{bucket_size}) +
                        $self->{hash_size}, $self->{long_size}));
                my $num = ord(substr($key, $tag->{ch} + 1, 1));

                if ($offsets[$num]) {
                    my $offset = $offsets[$num] + DBM::Deep->SIG_SIZE + $self->{data_size};
                    seek($fh, $offset + $root->{file_offset}, SEEK_SET);
                    my $subkeys;
                    read( $fh, $subkeys, $self->{bucket_list_size});

                    for (my $k=0; $k<$self->{max_buckets}; $k++) {
                        my $subloc = unpack($self->{long_pack}, substr($subkeys, ($k * $self->{bucket_size}) +
                                $self->{hash_size}, $self->{long_size}));
                        if (!$subloc) {
                            seek($fh, $offset + ($k * $self->{bucket_size}) + $root->{file_offset}, SEEK_SET);
                            print( $fh $key . pack($self->{long_pack}, $old_subloc || $root->{end}) );
                            last;
                        }
                    } # k loop
                }
                else {
                    $offsets[$num] = $root->{end};
                    seek($fh, $index_tag->{offset} + ($num * $self->{long_size}) + $root->{file_offset}, SEEK_SET);
                    print( $fh pack($self->{long_pack}, $root->{end}) );

                    my $blist_tag = $self->create_tag($obj, $root->{end}, DBM::Deep->SIG_BLIST, chr(0) x $self->{bucket_list_size});

                    seek($fh, $blist_tag->{offset} + $root->{file_offset}, SEEK_SET);
                    print( $fh $key . pack($self->{long_pack}, $old_subloc || $root->{end}) );
                }
            } # key is real
        } # i loop

        $location ||= $root->{end};
    } # re-index bucket list

    ##
    # Seek to content area and store signature, value and plaintext key
    ##
    if ($location) {
        my $content_length;
        seek($fh, $location + $root->{file_offset}, SEEK_SET);

        ##
        # Write signature based on content type, set content length and write actual value.
        ##
        my $r = Scalar::Util::reftype($value) || '';
        if ($r eq 'HASH') {
            print( $fh DBM::Deep->TYPE_HASH );
            print( $fh pack($self->{data_pack}, $self->{index_size}) . chr(0) x $self->{index_size} );
            $content_length = $self->{index_size};
        }
        elsif ($r eq 'ARRAY') {
            print( $fh DBM::Deep->TYPE_ARRAY );
            print( $fh pack($self->{data_pack}, $self->{index_size}) . chr(0) x $self->{index_size} );
            $content_length = $self->{index_size};
        }
        elsif (!defined($value)) {
            print( $fh DBM::Deep->SIG_NULL );
            print( $fh pack($self->{data_pack}, 0) );
            $content_length = 0;
        }
        else {
            print( $fh DBM::Deep->SIG_DATA );
            print( $fh pack($self->{data_pack}, length($value)) . $value );
            $content_length = length($value);
        }

        ##
        # Plain key is stored AFTER value, as keys are typically fetched less often.
        ##
        print( $fh pack($self->{data_pack}, length($plain_key)) . $plain_key );

        ##
        # If value is blessed, preserve class name
        ##
        if ( $root->{autobless} ) {
            my $value_class = Scalar::Util::blessed($value);
            if ( defined $value_class && $value_class ne 'DBM::Deep' ) {
                ##
                # Blessed ref -- will restore later
                ##
                print( $fh chr(1) );
                print( $fh pack($self->{data_pack}, length($value_class)) . $value_class );
                $content_length += 1;
                $content_length += $self->{data_size} + length($value_class);
            }
            else {
                print( $fh chr(0) );
                $content_length += 1;
            }
        }

        ##
        # If this is a new content area, advance EOF counter
        ##
        if ($location == $root->{end}) {
            $root->{end} += DBM::Deep->SIG_SIZE;
            $root->{end} += $self->{data_size} + $content_length;
            $root->{end} += $self->{data_size} + length($plain_key);
        }

        ##
        # If content is a hash or array, create new child DBM::Deep object and
        # pass each key or element to it.
        ##
        if ($r eq 'HASH') {
            my $branch = DBM::Deep->new(
                type => DBM::Deep->TYPE_HASH,
                base_offset => $location,
                root => $root,
            );
            foreach my $key (keys %{$value}) {
                $branch->STORE( $key, $value->{$key} );
            }
        }
        elsif ($r eq 'ARRAY') {
            my $branch = DBM::Deep->new(
                type => DBM::Deep->TYPE_ARRAY,
                base_offset => $location,
                root => $root,
            );
            my $index = 0;
            foreach my $element (@{$value}) {
                $branch->STORE( $index, $element );
                $index++;
            }
        }

        return $result;
    }

    $obj->_throw_error("Fatal error: indexing failed -- possibly due to corruption in file");
}

sub get_bucket_value {
    ##
    # Fetch single value given tag and MD5 digested key.
    ##
    my $self = shift;
    my ($obj, $tag, $md5) = @_;
    my $keys = $tag->{content};

    my $fh = $obj->_fh;

    ##
    # Iterate through buckets, looking for a key match
    ##
    BUCKET:
    for (my $i=0; $i<$self->{max_buckets}; $i++) {
        my $key = substr($keys, $i * $self->{bucket_size}, $self->{hash_size});
        my $subloc = unpack($self->{long_pack}, substr($keys, ($i * $self->{bucket_size}) + $self->{hash_size}, $self->{long_size}));

        if (!$subloc) {
            ##
            # Hit end of list, no match
            ##
            return;
        }

        if ( $md5 ne $key ) {
            next BUCKET;
        }

        ##
        # Found match -- seek to offset and read signature
        ##
        my $signature;
        seek($fh, $subloc + $obj->_root->{file_offset}, SEEK_SET);
        read( $fh, $signature, DBM::Deep->SIG_SIZE);

        ##
        # If value is a hash or array, return new DBM::Deep object with correct offset
        ##
        if (($signature eq DBM::Deep->TYPE_HASH) || ($signature eq DBM::Deep->TYPE_ARRAY)) {
            my $obj = DBM::Deep->new(
                type => $signature,
                base_offset => $subloc,
                root => $obj->_root,
            );

            if ($obj->_root->{autobless}) {
                ##
                # Skip over value and plain key to see if object needs
                # to be re-blessed
                ##
                seek($fh, $self->{data_size} + $self->{index_size}, SEEK_CUR);

                my $size;
                read( $fh, $size, $self->{data_size}); $size = unpack($self->{data_pack}, $size);
                if ($size) { seek($fh, $size, SEEK_CUR); }

                my $bless_bit;
                read( $fh, $bless_bit, 1);
                if (ord($bless_bit)) {
                    ##
                    # Yes, object needs to be re-blessed
                    ##
                    my $class_name;
                    read( $fh, $size, $self->{data_size}); $size = unpack($self->{data_pack}, $size);
                    if ($size) { read( $fh, $class_name, $size); }
                    if ($class_name) { $obj = bless( $obj, $class_name ); }
                }
            }

            return $obj;
        }

        ##
        # Otherwise return actual value
        ##
        elsif ($signature eq DBM::Deep->SIG_DATA) {
            my $size;
            my $value = '';
            read( $fh, $size, $self->{data_size}); $size = unpack($self->{data_pack}, $size);
            if ($size) { read( $fh, $value, $size); }
            return $value;
        }

        ##
        # Key exists, but content is null
        ##
        else { return; }
    } # i loop

    return;
}

sub delete_bucket {
    ##
    # Delete single key/value pair given tag and MD5 digested key.
    ##
    my $self = shift;
    my ($obj, $tag, $md5) = @_;
    my $keys = $tag->{content};

    my $fh = $obj->_fh;

    ##
    # Iterate through buckets, looking for a key match
    ##
    BUCKET:
    for (my $i=0; $i<$self->{max_buckets}; $i++) {
        my $key = substr($keys, $i * $self->{bucket_size}, $self->{hash_size});
        my $subloc = unpack($self->{long_pack}, substr($keys, ($i * $self->{bucket_size}) + $self->{hash_size}, $self->{long_size}));

        if (!$subloc) {
            ##
            # Hit end of list, no match
            ##
            return;
        }

        if ( $md5 ne $key ) {
            next BUCKET;
        }

        ##
        # Matched key -- delete bucket and return
        ##
        seek($fh, $tag->{offset} + ($i * $self->{bucket_size}) + $obj->_root->{file_offset}, SEEK_SET);
        print( $fh substr($keys, ($i+1) * $self->{bucket_size} ) );
        print( $fh chr(0) x $self->{bucket_size} );

        return 1;
    } # i loop

    return;
}

sub bucket_exists {
    ##
    # Check existence of single key given tag and MD5 digested key.
    ##
    my $self = shift;
    my ($obj, $tag, $md5) = @_;
    my $keys = $tag->{content};

    ##
    # Iterate through buckets, looking for a key match
    ##
    BUCKET:
    for (my $i=0; $i<$self->{max_buckets}; $i++) {
        my $key = substr($keys, $i * $self->{bucket_size}, $self->{hash_size});
        my $subloc = unpack($self->{long_pack}, substr($keys, ($i * $self->{bucket_size}) + $self->{hash_size}, $self->{long_size}));

        if (!$subloc) {
            ##
            # Hit end of list, no match
            ##
            return;
        }

        if ( $md5 ne $key ) {
            next BUCKET;
        }

        ##
        # Matched key -- return true
        ##
        return 1;
    } # i loop

    return;
}

sub find_bucket_list {
    ##
    # Locate offset for bucket list, given digested key
    ##
    my $self = shift;
    my ($obj, $md5, $args) = @_;
    $args = {} unless $args;

    ##
    # Locate offset for bucket list using digest index system
    ##
    my $tag = $self->load_tag($obj, $obj->_base_offset)
        or $self->_throw_error( "INTERNAL ERROR - Cannot find tag" );
#print $obj->_base_offset, " : $tag->{signature} : $tag->{offset} : $tag->{size}\n";

    my $ch = 0;
    while ($tag->{signature} ne DBM::Deep->SIG_BLIST) {
        my $num = ord substr($md5, $ch, 1);

        my $ref_loc = $tag->{offset} + ($num * $self->{long_size});
        $tag = $self->index_lookup( $obj, $tag, $num );

        if (!$tag) {
            if ( $args->{create} ) {
                my $fh = $obj->_fh;
                seek($fh, $ref_loc + $obj->_root->{file_offset}, SEEK_SET);
                print( $fh pack($self->{long_pack}, $obj->_root->{end}) );

                $tag = $self->create_tag(
                    $obj, $obj->_root->{end},
                    DBM::Deep->SIG_BLIST,
                    chr(0) x $self->{bucket_list_size},
                );

                $tag->{ref_loc} = $ref_loc;
                $tag->{ch} = $ch;

                last;
            }
            else {
                return;
            }
        }

        $tag->{ch} = $ch;
        $tag->{ref_loc} = $ref_loc;

        $ch++;
    }

    return $tag;
}

sub index_lookup {
    ##
    # Given index tag, lookup single entry in index and return .
    ##
    my $self = shift;
    my ($obj, $tag, $index) = @_;

    my $location = unpack(
        $self->{long_pack},
        substr(
            $tag->{content},
            $index * $self->{long_size},
            $self->{long_size},
        ),
    );

    if (!$location) { return; }

    return $self->load_tag( $obj, $location );
}

sub traverse_index {
    ##
    # Scan index and recursively step into deeper levels, looking for next key.
    ##
    my $self = shift;
    my ($obj, $offset, $ch, $force_return_next) = @_;

    my $tag = $self->load_tag($obj, $offset );

    my $fh = $obj->_fh;

    if ($tag->{signature} ne DBM::Deep->SIG_BLIST) {
        my $content = $tag->{content};
        my $start = $obj->{return_next} ? 0 : ord(substr($obj->{prev_md5}, $ch, 1));

        for (my $index = $start; $index < 256; $index++) {
            my $subloc = unpack(
                $self->{long_pack},
                substr($content, $index * $self->{long_size}, $self->{long_size}),
            );

            if ($subloc) {
                my $result = $self->traverse_index(
                    $obj, $subloc, $ch + 1, $force_return_next,
                );

                if (defined($result)) { return $result; }
            }
        } # index loop

        $obj->{return_next} = 1;
    } # tag is an index

    else {
        my $keys = $tag->{content};
        if ($force_return_next) { $obj->{return_next} = 1; }

        ##
        # Iterate through buckets, looking for a key match
        ##
        for (my $i=0; $i<$self->{max_buckets}; $i++) {
            my $key = substr($keys, $i * $self->{bucket_size}, $self->{hash_size});
            my $subloc = unpack(
                $self->{long_pack},
                substr(
                    $keys,
                    ($i * $self->{bucket_size}) + $self->{hash_size},
                    $self->{long_size},
                ),
            );

            if (!$subloc) {
                ##
                # End of bucket list -- return to outer loop
                ##
                $obj->{return_next} = 1;
                last;
            }
            elsif ($key eq $obj->{prev_md5}) {
                ##
                # Located previous key -- return next one found
                ##
                $obj->{return_next} = 1;
                next;
            }
            elsif ($obj->{return_next}) {
                ##
                # Seek to bucket location and skip over signature
                ##
                seek($fh, $subloc + DBM::Deep->SIG_SIZE + $obj->_root->{file_offset}, SEEK_SET);

                ##
                # Skip over value to get to plain key
                ##
                my $size;
                read( $fh, $size, $self->{data_size});
                $size = unpack($self->{data_pack}, $size);
                if ($size) { seek($fh, $size, SEEK_CUR); }

                ##
                # Read in plain key and return as scalar
                ##
                my $plain_key;
                read( $fh, $size, $self->{data_size});
                $size = unpack($self->{data_pack}, $size);
                if ($size) { read( $fh, $plain_key, $size); }

                return $plain_key;
            }
        } # bucket loop

        $obj->{return_next} = 1;
    } # tag is a bucket list

    return;
}

sub get_next_key {
    ##
    # Locate next key, given digested previous one
    ##
    my $self = shift;
    my ($obj) = @_;

    $obj->{prev_md5} = $_[1] ? $_[1] : undef;
    $obj->{return_next} = 0;

    ##
    # If the previous key was not specifed, start at the top and
    # return the first one found.
    ##
    if (!$obj->{prev_md5}) {
        $obj->{prev_md5} = chr(0) x $self->{hash_size};
        $obj->{return_next} = 1;
    }

    return $self->traverse_index( $obj, $obj->_base_offset, 0 );
}

1;
__END__