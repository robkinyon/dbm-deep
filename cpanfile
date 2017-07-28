requires 'Fcntl';
requires 'Scalar::Util';
requires 'Digest::MD5';
requires 'DBI' => '1.5';
requires 'DBD::SQLite' => '1.25';
#requires 'DBD::mysql' => '1.25';

on test => sub {
  requires 'File::Path'      => '0.01';
  requires 'File::Temp'      => '0.01';
  requires 'Pod::Usage'      => '1.3';
  requires 'Test::Deep'      => '0.095';
  requires 'Test::Warn'      => '0.08';
  requires 'Test::More'      => '0.88'; # done_testing
  requires 'Test::Exception' => '0.21';
};
