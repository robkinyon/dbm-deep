package DBM::Deep::SQL::Hash;

use strict;
use warnings FATAL => 'all';

BEGIN {
	use base 'DBM::Deep::SQL::Util';

	use Digest::MD5 'md5_base64';
	use Storable 'nfreeze', 'thaw';
}

sub _get_self
{
	eval { local $SIG{'__DIE__'}; tied( %{$_[0]} ) } || $_[0];
}

sub _clear
{
	my ($obj) = @_;
	my $ks = $obj->_get_keys();
	foreach my $k (@$ks)
	{
		$obj->_delete($k);
	}
	$obj->{'cache'} = {};
}

sub _get_keys
{
	my ($obj) = @_;
	if (exists $obj->{'keys'})
	{
		my @ks = keys %{$obj->{'keys'}};
		return (wantarray()) ? @ks : \@ks;
	}
	my $q = $obj->_select(
		'table' => 'rec_hash_item',
		'fields' => ['key_type', 'key_data'],
		'where' => {
			'hash' => $obj->{'id'},
		},
	);
	my @textkey = ();
	my @textkeypos = ();
	my $kcache = $obj->{'keys'} = {};
	my @ks = ();
	foreach my $i (0..$#{$q})
	{
		my $row = $q->[$i];
		my $kt = $row->[0];
		my $k = $row->[1];
		if ($kt eq 'text')
		{
			push @ks, undef;
			push @textkey, $k;
			push @textkeypos, $i;
		}
		else
		{
			push @ks, $k;
			$kcache->{$k} = undef;
		}
	}
	if (scalar @textkey)
	{
		my $ids = join(',', @textkey);
		my $tq = $obj->_select(
			'table' => 'rec_value_text',
			'fields' => ['id', 'data'],
			'where' => "id in ($ids)",
		);
		my %data = map {$_->[0] => $_->[1]} @$tq;
		foreach my $x (0..$#textkey)
		{
			my $id = $textkey[$x];
			my $i = $textkeypos[$x];
			my $nk = $data{$id};
			$ks[$i] = $nk;
			$kcache->{$nk} = undef;
		}
	}
	return (wantarray()) ? @ks : \@ks;
}

sub _delete
{
	my ($obj, $k) = @_;
	my $hcode = md5_base64($k);
	my $q = $obj->_select(
		'table' => 'rec_hash_item',
		'fields' => ['value_type', 'value_data', 'id', 'key_type', 'key_data'],
		'where' => {
			'hash' => $obj->{'id'},
			'key_hash' => $hcode,
		},
	);
	if (scalar @$q)
	{
		my $kt = $q->[0]->[3];
		if ($kt eq 'text')
		{
			$obj->_delete_sql('rec_value_text', {'id' => $q->[0]->[4]});
		}
		my $dt = $q->[0]->[0];
		if ($dt eq 'text' || $dt eq 'data')
		{
			$obj->_delete_sql('rec_value_'. $dt, {'id' => $q->[0]->[1]});
		}
		elsif ($dt eq 'hash')
		{
			my $rec = $obj->_tiehash($q->[0]->[1]);
			%$rec = ();
			$obj->_delete_sql('rec_hash', {'id' => $q->[0]->[1]});
			$obj->_delete_sql('rec_item', {'id' => $q->[0]->[1]});
		}
		elsif ($dt eq 'array')
		{
			my $rec = $obj->_tiearray($q->[0]->[1]);
			@$rec = ();
			$obj->_delete_sql('rec_array', {'id' => $q->[0]->[1]});
			$obj->_delete_sql('rec_item', {'id' => $q->[0]->[1]});
		}
		$obj->_delete_sql('rec_hash_item', {'id' => $q->[0]->[2]});
	}
	delete $obj->{'cache'}->{$k};
	if (exists $obj->{'keys'})
	{
		delete $obj->{'keys'}->{$k};
	}
}

sub _set_cache
{
	my ($obj, $k, $val) = @_;
	$obj->{'cache'}->{$k} = $val;
	if (exists $obj->{'keys'})
	{
		$obj->{'keys'}->{$k} = undef;
	}
}

sub _get_cache
{
	my ($obj, $k, $vref) = @_;
	if (exists $obj->{'cache'}->{$k})
	{
		$$vref = $obj->{'cache'}->{$k};
		return 1;
	}
	return undef;
}

sub _exists
{
	my ($obj, $k) = @_;
	if (exists $obj->{'cache'}->{$k})
	{
		return 1;
	}
	my $hcode = md5_base64($k);
	my $c = $obj->_select(
		'table' => 'rec_hash_item',
		'fields' => 'count(id)',
		'where' => {
			'hash' => $obj->{'id'},
			'key_hash' => $hcode,
		},
	)->[0]->[0];
	return $c;
}

sub _data
{
	my ($obj, $k) = @_;
	my $hcode = md5_base64($k);
	my $q = $obj->_select(
		'table' => 'rec_hash_item',
		'fields' => ['value_type', 'value_data'],
		'where' => {
			'hash' => $obj->{'id'},
			'key_hash' => $hcode,
		},
	);
	if (scalar @$q)
	{
		my $dt = $q->[0]->[0];
		my $val = $q->[0]->[1];
		if ($dt eq 'value')
		{
			return $val;
		}
		elsif ($dt eq 'text')
		{
			my $dq = $obj->_select(
				'table' => 'rec_value_text',
				'fields' => 'data',
				'where' => {
					'id' => $val,
				},
			);
			return $dq->[0]->[0];
		}
		elsif ($dt eq 'data')
		{
			my $dq = $obj->_select(
				'table' => 'rec_value_data',
				'fields' => 'data',
				'where' => {
					'id' => $val,
				},
			);
			if (scalar @$dq)
			{
				my $rec = thaw($dq->[0]->[0]);
				return $rec;
			}
			return undef;
		}
		elsif ($dt eq 'array')
		{
			my $rec = $obj->_tiearray($val);
			if ($obj->{'prefetch'})
			{
				(tied(@$rec))->_prefetch();
			}
			return $rec;
		}
		elsif ($dt eq 'hash')
		{
			my $rec = $obj->_tiehash($val);
			if ($obj->{'prefetch'})
			{
				(tied(%$rec))->_prefetch();
			}
			return $rec;
		}
	}
	return undef;
}

sub _prefetch
{
	my ($obj) = @_;
	my $pd = $obj->_select(
		'table' => 'rec_hash_item',
		'fields' => ['key_type', 'key_data', 'value_type', 'value_data'],
		'where' => {
			'hash' => $obj->{'id'},
		},
	);
	my @data = ();
	my @datapos = ();
	my @text = ();
	my @textpos = ();
	my %hash = ();
	my @textkey = ();
	my @textkeypos = ();
	foreach my $i (0..$#{$pd})
	{
		my $row = $pd->[$i];
		my $kt = $row->[0];
		my $k = $row->[1];
		if ($kt eq 'text')
		{
			push @textkey, $k;
			push @textkeypos, $i;
		}
	}
	if (scalar @textkey)
	{
		my $ids = join(',', @textkey);
		my $tq = $obj->_select(
			'table' => 'rec_value_text',
			'fields' => ['id', 'data'],
			'where' => "id in ($ids)",
		);
		my %data = map {$_->[0] => $_->[1]} @$tq;
		foreach my $x (0..$#textkey)
		{
			my $id = $textkey[$x];
			my $i = $textkeypos[$x];
			$pd->[$i]->[1] = $data{$id};
		}
	}
	foreach my $r (@$pd)
	{
		my $k = $r->[1];
		my $vt = $r->[2];
		my $val = $r->[3];
		if ($vt eq 'value')
		{
			$hash{$k} = $val;
		}
		elsif ($vt eq 'text')
		{
			push @textpos, $k;
			push @text, $val;
		}
		elsif ($vt eq 'value')
		{
			push @datapos, $k;
			push @data, $val;
		}
		elsif ($vt eq 'array')
		{
			my $rec = $obj->_tiearray($val);
			if ($obj->{'prefetch'})
			{
				(tied(@$rec))->_prefetch();
			}
			$hash{$k} = $rec;
		}
		elsif ($vt eq 'hash')
		{
			my $rec = $obj->_tiehash($val);
			if ($obj->{'prefetch'})
			{
				(tied(@$rec))->_prefetch();
			}
			$hash{$k} = $rec;
		}
	}
	if (scalar @text)
	{
		my $ids = join(',', @text);
		my $tq = $obj->_select(
			'table' => 'rec_value_text',
			'fields' => ['id', 'data'],
			'where' => "id in ($ids)",
		);
		my %data = map {$_->[0] => $_->[1]} @$tq;
		foreach my $x (0..$#text)
		{
			my $id = $text[$x];
			my $k = $textpos[$x];
			$hash{$k} = $data{$id};
		}
	}
	if (scalar @data)
	{
		my $ids = join(',', @data);
		my $tq = $obj->_select(
			'table' => 'rec_value_data',
			'fields' => ['id', 'data'],
			'where' => "id in ($ids)",
		);
		my %d = map {$_->[0] => $_->[1]} @$tq;
		foreach my $x (0..$#data)
		{
			my $id = $data[$x];
			my $k = $datapos[$x];
			if (defined $d{$id})
			{
				$hash{$k} = thaw($d{$id});
			}
		}
	}
	return $obj->{'cache'} = \%hash;
}

sub TIEHASH
{
	my $class = shift;
	my %prm = @_;
	my $obj = \%prm;
	$obj->{'sort'} = 1;
	$obj->{'cache'} = {};
	bless $obj, $class;
	return $obj;
}

sub FETCH
{
	my ($tobj, $k) = @_;
	my $obj = $tobj->_get_self();
	my $val = undef;
	if ($obj->_get_cache($k, \$val))
	{
		return $val;
	}
	$val = $obj->_data($k);
	if (defined $val)
	{
		$obj->_set_cache($k, $val);
	}
	return $val;
}

sub STORE
{
	my ($tobj, $k, $val) = @_;
	my $dval = $val;
	my $obj = $tobj->_get_self();
	my $vt;
	$val = '' unless (defined $val);
	if (ref $val) {
		my $done = 0;
		unless ($obj->{'serialize'}) {
			if ($val =~ /HASH/) {
				my $id = $obj->_create('hash');
				my $ta = $obj->_tiehash($id);
				$dval = $ta;
				foreach my $k (keys %$val) {
					$ta->{$k} = $val->{$k};
				}
 				$vt = 'hash';
				$val = $id;
				$done = 1;
			}
			elsif ($val =~ /ARRAY/) {
				my $id = $obj->_create('array');
				my $ta = $obj->_tiearray($id);
				$dval = $ta;
				foreach my $i (0..$#{$val}) {
					$ta->[$i] = $val->[$i];
				}
 				$vt = 'array';
				$val = $id;
				$done = 1;
			}
		}
		unless ($done) {
			my $data = nfreeze($val);
			$val = $obj->_create('value_data', {
				'data' => $data,
			});
 			$vt = 'data';
		}
	}
	elsif (length($val) > 255) {
		$val = $obj->_create('value_data', {
			'data' => $val,
		});
 		$vt = 'text';
	}
	else {
 		$vt = 'value';
	}
	my $hcode = md5_base64($k);
	my $c = $obj->_select(
		'table' => 'rec_hash_item',
		'fields' => ['value_type', 'id'],
		'where' => {
			'hash' => $obj->{'id'},
			'key_hash' => $hcode,
		},
	);
	my $create = 1;
	if (scalar @$c) {
		if ($c->[0]->[0] eq 'value') {
			$create = 0;
			$obj->_update(
				'table' => 'rec_hash_item',
				'fields' => {
					'value_type' => $vt,
					'value_data' => $val,
				},
				'where' => {
					'id' => $c->[0]->[1],
				},
			);
		}
		else {
			$obj->_delete($k);
		}
	}
	if ($create) {
		my $kt;
		if (length($k) > 255) {
			$k = $obj->_create('value_text', {
				'data' => $k,
			});
			$kt = 'text';
		}
		else {
			$kt = 'value';
		}
		$obj->_create('hash_item', {
			'hash' => $obj->{'id'},
			'key_hash' => $hcode,
			'key_data' => $k,
			'key_type' => $kt,
			'value_data' => $val,
			'value_type' => $vt,
		});
	}
	$obj->_set_cache($k, $dval);
	return $dval;
}

sub EXISTS
{
	my ($tobj, $k) = @_;
	my $obj = $tobj->_get_self();
	$k = '' unless defined ($k);
	return $obj->_exists($k);
}

sub DELETE
{
	my ($tobj, $i) = @_;
	my $obj = $tobj->_get_self();
	$obj->_delete($i);
}

sub CLEAR
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	$obj->_clear();
}

sub FIRSTKEY
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	if ($obj->{'sort'})
	{
		$obj->{'keys_sorted'} = [sort $obj->_get_keys()];
		return shift @{$obj->{'keys_sorted'}};
	}
	else
	{
		$obj->_get_keys();
		return each %{$obj->{'keys'}};
	}
}

sub NEXTKEY
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	if ($obj->{'sort'} && exists $obj->{'keys_sorted'})
	{
		return shift @{$obj->{'keys_sorted'}};
	}
	else
	{
		return each %{$obj->{'keys'}};
	}
}

sub SCALAR
{
	# TODO
}

sub id
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	return $obj->{'id'};
}

1;

