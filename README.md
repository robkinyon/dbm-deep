DBM::Deep
---------

DBM::Deep is a pure Perl multi-level hash/array DBM that supports
transactions and offers a unique flat-file database system. Its
main features are:

 * true multi-level hash/array support (unlike MLDBM, which is faked)
 * hybrid OO / tie() interface
 * cross-platform FTPable files
 * ACID transactions
 * written in Pure-Perl: runs on Unix, OS X and Windows
 * high performance

DBM::Deep is **quite fast**, able to handle millions of keys and unlimited
levels without significant slow-downs.

It was written from the ground-up in pure perl - this is NOT a wrapper around
a C-based DBM. Because of that, we provide out-of-the-box compatibility with
Unix, Mac OS X and Windows.


#### Basic Usage ####

```perl
      use DBM::Deep;
      my $db = DBM::Deep->new( "foo.db" );

      $db->{key} = 'value';
      print $db->{key};

      $db->put('key' => 'value');
      print $db->get('key');

      # true multi-level support
      $db->{my_complex} = [
          'hello', { perl => 'rules' },
          42, 99,
      ];

      $db->begin_work;

      # Do stuff here

      $db->rollback;
      $db->commit;

      tie my %db, 'DBM::Deep', 'foo.db';
      $db{key} = 'value';
      print $db{key};

      tied(%db)->put('key' => 'value');
      print tied(%db)->get('key');
```

#### Installation ####

    cpanm DBM::Deep

or manually:

    perl Build.PL
    make test
    make install


#### More Information ####

Please refer to the
[complete DBM::Deep documentation](https://metacpan.org/pod/distribution/DBM-Deep/lib/DBM/Deep.pod)
online for more information on how to use DBM::Deep. After installing this module,
you'll also be able to reach that documentation locally by typing `perldoc DBM::Deep`.

We also have collected several interesting recipes on the
[DBM::Deep Cookbook](https://metacpan.org/pod/distribution/DBM-Deep/lib/DBM/Deep/Cookbook.pod),

and you still have questions or concerns you can check the DBM::Deep
Google Group at http://groups.google.com/group/DBM-Deep,
or send an email to DBM-Deep@googlegroups.com.

Finally, You can also visit #dbm-deep on irc.perl.org.

The source code repository is at http://github.com/robkinyon/dbm-deep

#### Maintainers ####

Rob Kinyon, rkinyon@cpan.org

Originally written by Joseph Huckaby, jhuckaby@cpan.org

#### Sponsors ####

Stonehenge Consulting (<http://www.stonehenge.com/>) sponsored the
development of transactions and freespace management, leading to the
1.0000 release. A great debt of gratitude goes out to them for their
continuing leadership in and support of the Perl community.

#### Contributors ####

The following have contributed greatly to make DBM::Deep what it is
today:

 * Adam Sah and Rich Gaushell for innumerable contributions early on.
 * Dan Golden and others at YAPC::NA 2006 for helping me design through transactions.
 * James Stanley for bug fix
 * David Steinbrunner for fixing typos and adding repository cpan metadata
 * H. Merijn Brandt for fixing the POD escapes.

#### Copyright and License ####

Copyright (c) 2007-2015 Rob Kinyon. All Rights Reserved. This is free
software, you may use it and distribute it under the same terms as Perl
itself.

