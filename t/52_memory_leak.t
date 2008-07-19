
# This was discussed here:
# http://groups.google.com/group/DBM-Deep/browse_thread/thread/a6b8224ffec21bab
# brought up by Alex Gallichotte

use strict;
use Test;
use DBM::Deep;
use t::common qw( new_fh );

my ($fh, $filename) = new_fh();
my $db = DBM::Deep->new( file => $filename, fh => $fh, );

my $todo = 1000;

$db->{randkey()} = 1 for 1 .. 1000;

plan tests => $todo*2;

my $error_count = 0;
my @mem = (mem(0), mem(1));
for my $i (1 .. $todo) {
    $db->{randkey()} = [@mem];

    print STDERR " @mem     \r";

    my @tm = (mem(0), mem(1));

    skip( not($mem[0]), $tm[0] <= $mem[0] );
    skip( not($mem[1]), $tm[1] <= $mem[1] );

    $error_count ++ if $tm[0] > $mem[0] or $tm[1] > $mem[1];
    die " ERROR: that's enough failures to prove the point ... " if $error_count > 20;

    @mem = @tm;
}

sub randkey {
    our $i ++;
    my @k = map { int rand 100 } 1 .. 10;
    local $" = "-";

    return "$i-@k";
}

sub mem {
    open my $in, "/proc/$$/statm" or return 0;
    my $line = [ split m/\s+/, <$in> ];
    close $in;

    return $line->[shift];
}

__END__
/proc/[number]/statm

      Provides information about memory status in pages.  The columns are:

          size       total program size
          resident   resident set size
          share      shared pages
          text       text (code)
          lib        library
          data       data/stack
          dt         dirty pages (unused in Linux 2.6)