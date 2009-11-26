#!/usr/bin/perl

use DBM::Deep;
use Data::Dumper;

my $hash = new DBM::Deep(
	'dbi' => {
		'dsn' => 'DBI:mysql:database=perl;host=localhost',
		'user' => 'perl',
		'password' => '2A7Qcmh5CBQvLGUu',
	},
	'id' => 20,
);

print Dumper(
	$hash,
	$hash->id(),
);

my $array = new DBM::Deep(
	'dbi' => {
		'dsn' => 'DBI:mysql:database=perl;host=localhost',
		'user' => 'perl',
		'password' => '2A7Qcmh5CBQvLGUu',
	},
	'type' => DBM::Deep->TYPE_ARRAY,
	'id' => 21,
);

print Dumper(
	$array,
	$array->id(),
);

