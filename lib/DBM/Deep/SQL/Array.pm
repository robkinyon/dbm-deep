package DBM::Deep::SQL::Array;

use strict;
use warnings FATAL => 'all';

BEGIN {
	use base 'DBM::Deep::SQL::Util';

	use Storable 'nfreeze', 'thaw';
}

sub _get_self
{
	eval { local $SIG{'__DIE__'}; tied( @{$_[0]} ) } || $_[0];
}

sub _size
{
	my ($obj) = @_;
	my $sq = $obj->_select(
		'table' => 'rec_array_item',
		'fields' => 'max(pos)',
		'where' => {
			'array' => $obj->{'id'},
		},
	);
	if (defined $sq->[0]->[0])
	{
		return $sq->[0]->[0] + 1;
	}
	return 0;
}

sub _clear
{
	my ($obj) = @_;
	my $sz = $obj->_size();
	foreach my $i (1..$sz)
	{
		$obj->_delete($i - 1);
	}
	$obj->{'cache'} = [];
}

sub _delete
{
	my ($obj, $i) = @_;
	my $q = $obj->_select(
		'table' => 'rec_array_item',
		'fields' => ['value_type', 'value_data', 'id'],
		'where' => {
			'array' => $obj->{'id'},
			'pos' => $i,
		},
	);
	if (scalar @$q)
	{
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
		$obj->_delete_sql('rec_array_item', {'id' => $q->[0]->[2]});
	}
	delete $obj->{'cache'}->[$i];
}

sub _set_cache
{
	my ($obj, $pos, $val) = @_;
	$obj->{'cache'}->[$pos] = $val;
}

sub _get_cache
{
	my ($obj, $pos, $vref) = @_;
	if (exists $obj->{'cache'}->[$pos])
	{
		$$vref = $obj->{'cache'}->[$pos];
		return 1;
	}
	return undef;
}

sub _exists
{
	my ($obj, $i) = @_;
	if (exists $obj->{'cache'}->[$i])
	{
		return 1;
	}
	my $c = $obj->_select(
		'table' => 'rec_array_item',
		'fields' => 'count(id)',
		'where' => {
			'array' => $obj->{'id'},
			'pos' => $i,
		},
	)->[0]->[0];
	return $c;
}

sub _data
{
	my ($obj, $i) = @_;
	my $q = $obj->_select(
		'table' => 'rec_array_item',
		'fields' => ['value_type', 'value_data'],
		'where' => {
			'array' => $obj->{'id'},
			'pos' => $i,
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

sub _tiearray
{
	my ($obj, $id) = @_;
	my $rec = undef;
	tie(@$rec, 'DBM::Deep::SQL::Array', (
		'dbi' => $obj->{'dbi'},
		'id' => $id,
		'prefetch' => $obj->{'prefetch'},
	));
	bless $rec, 'DBM::Deep::SQL::Array';
	return $rec;
}

sub _tiehash
{
	my ($obj, $id) = @_;
	my $rec = undef;
	tie(%$rec, 'DBM::Deep::SQL::Hash', (
		'dbi' => $obj->{'dbi'},
		'id' => $id,
		'prefetch' => $obj->{'prefetch'},
	));
	bless $rec, 'DBM::Deep::SQL::Hash';
	return $rec;
}

sub _prefetch
{
	my ($obj) = @_;
	my $pd = $obj->_select(
		'table' => 'rec_array_item',
		'fields' => ['pos', 'value_type', 'value_data'],
		'where' => {
			'array' => $obj->{'id'},
		},
	);
	my @data = ();
	my @datapos = ();
	my @text = ();
	my @textpos = ();
	my @array = ();
	foreach my $r (@$pd)
	{
		my $i = $r->[0];
		my $vt = $r->[1];
		my $val = $r->[2];
		if ($vt eq 'value')
		{
			$array[$i] = $val;
		}
		elsif ($vt eq 'text')
		{
			push @textpos, $i;
			push @text, $val;
		}
		elsif ($vt eq 'data')
		{
			push @datapos, $i;
			push @data, $val;
		}
		elsif ($vt eq 'array')
		{
			my $rec = $obj->_tiearray($val);
			if ($obj->{'prefetch'})
			{
				(tied(@$rec))->_prefetch();
			}
			$array[$i] = $rec;
		}
		elsif ($vt eq 'hash')
		{
			my $rec = $obj->_tiehash($val);
			if ($obj->{'prefetch'})
			{
				(tied(@$rec))->_prefetch();
			}
			$array[$i] = $rec;
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
			my $i = $textpos[$x];
			$array[$i] = $data{$id};
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
			my $i = $datapos[$x];
			if (defined $d{$id})
			{
				$array[$i] = thaw($d{$id});
			}
		}
	}
	return $obj->{'cache'} = \@array;
}

sub TIEARRAY
{
	my $class = shift;
	my %prm = @_;
	my $obj = \%prm;
	$obj->{'cache'} = [];
	bless $obj, $class;
	return $obj;
}

sub FETCH
{
	my ($tobj, $i) = @_;
	my $obj = $tobj->_get_self();
	my $val = undef;
	if ($obj->_get_cache($i, \$val))
	{
		return $val;
	}
	$val = $obj->_data($i);
	if (defined $val)
	{
		$obj->_set_cache($i, $val);
	}
	return $val;
}

sub STORE
{
	my ($tobj, $i, $val) = @_;
	my $obj = $tobj->_get_self();
	my $dval = $val;
	my $vt;
	$val = '' unless (defined $val);
	if (ref $val)
	{
		my $done = 0;
		unless ($obj->{'serialize'})
		{
			if ($val =~ /HASH/)
			{
				my $id = $obj->_create('hash');
				my $ta = $obj->_tiehash($id);
				$dval = $ta;
				foreach my $k (keys %$val)
				{
					$ta->{$k} = $val->{$k};
				}
 				$vt = 'hash';
				$val = $id;
				$done = 1;
			}
			elsif ($val =~ /ARRAY/)
			{
				my $id = $obj->_create('array');
				my $ta = $obj->_tiearray($id);
				$dval = $ta;
				foreach my $i (0..$#{$val})
				{
					$ta->[$i] = $val->[$i];
				}
 				$vt = 'array';
				$val = $id;
				$done = 1;
			}
		}
		unless ($done)
		{
			my $data = nfreeze($val);
			$val = $obj->_create('value_data', {
				'data' => $data,
			});
 			$vt = 'data';
		}
	}
	elsif (length($val) > 255)
	{
		$val = $obj->_create('value_text', {
			'data' => $val,
		});
 		$vt = 'text';
	}
	else
	{
 		$vt = 'value';
	}
	my $c = $obj->_select(
		'table' => 'rec_array_item',
		'fields' => ['value_type', 'id'],
		'where' => {
			'array' => $obj->{'id'},
			'pos' => $i,
		},
	);
	my $create = 1;
	if (scalar @$c)
	{
		if ($c->[0]->[0] eq 'value')
		{
			$create = 0;
			$obj->_update(
				'table' => 'rec_array_item',
				'fields' => {
					'value_type' => $vt,
					'value_data' => $val,
				},
				'where' => {
					'id' => $c->[0]->[1],
				},
			);
		}
		else
		{
			$obj->_delete($i);
		}
	}
	if ($create)
	{
		$obj->_create('array_item', {
			'array' => $obj->{'id'},
			'pos' => $i,
			'value_data' => $val,
			'value_type' => $vt,
		});
	}
	$obj->_set_cache($i, $dval);
	return $dval;
}

sub FETCHSIZE
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	return $obj->_size();
}

sub EXISTS
{
	my ($tobj, $i) = @_;
	my $obj = $tobj->_get_self();
	return $obj->_exists($i);
}

sub DELETE
{
	my ($tobj, $i) = @_;
	my $obj = $tobj->_get_self();
	return $obj->_delete($i);
}

sub CLEAR
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	return $obj->_clear();
}

sub PUSH
{
	my ($tobj, @list) = @_;
	my $obj = $tobj->_get_self();
	my $last = $obj->_size();
	foreach my $i (0..$#list)
	{
		$tobj->STORE($last + $i, $list[$i]);
	}
	return $obj->_size();
}

sub POP
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	my $top = $obj->_size();
	unless ($top > 0)
	{
		return undef;
	}
	my $val = $obj->_data($top - 1);
	$obj->_delete($top - 1);
	return $val;
}

sub SHIFT
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	my $top = $obj->_size();
	unless ($top > 0)
	{
		return undef;
	}
	my $val = $obj->_data(0);
	$obj->_delete(0);
	my $sql = 'update rec_array_item set pos=pos-1 where array=? order by pos asc';
	$obj->{'dbi'}->query($sql, $obj->{'id'});
	return $val;
}

sub UNSHIFT
{
	my ($tobj, $val) = @_;
	my $obj = $tobj->_get_self();
	my $top = $obj->_size();
	if ($top > 0)
	{
		my $sql = 'update rec_array_item set pos=pos+1 where array=? order by pos desc';
		$obj->{'dbi'}->query($sql, $obj->{'id'});
	}
	return $tobj->STORE(0, $val);
}

sub EXTEND
{
	# Not needed
	return;
}

sub SPLICE
{
	my ($tobj, $offset, $len, @list) = @_;
	my $obj = $tobj->_get_self();
	my $cache = $obj->{'cache'};
	$obj->{'cache'} = [];
	unless (defined $offset)
	{
		$offset = 0;
	}
	if (length($offset) < 0)
	{
		die('Splice with negative offset not supported'); # TODO
	}
	unless (defined $len)
	{
		$len = $obj->_size() - $offset;
	}
	if (length($len) < 0)
	{
		die('Splice with negative length not supported'); # TODO
	}
	if ($offset < $#{$cache} || $offset == 0)
	{
		splice(@$cache, $offset, $len, @list);
	}
	else
	{
		$cache = [];
	}
	my $lc = (wantarray) ? 1 : 0;
	my @res = ();
	if ($len > 0)
	{
		foreach my $i (0..($len - 1))
		{
			my $k = $offset + $i;
			if ($lc || $i == ($len - 1))
			{
				my $rc = $tobj->FETCH($k);
				my $cl = $obj->_clone_tree($rc);
				push @res, $cl;
			}
			$obj->_delete($k);
		}
	}
	my $elems = scalar @list;
	my $diff = $elems - $len;
	if ($elems > 0 || $diff < 0)
	{
		my $st = $offset + $len - 1;
		my $dir = ($diff > 0) ? 'desc' : 'asc';
		my $sql = 'update rec_array_item set pos=pos+? where array=? and pos > ? order by pos '. $dir;
		$obj->{'dbi'}->query($sql, $diff, $obj->{'id'}, $st);
		foreach my $i (0..$#list)
		{
			$tobj->STORE($offset + $i, $list[$i]);
		}
	}
	$obj->{'cache'} = $cache;
	if ($lc)
	{
		return @res;
	}
	else
	{
		return $res[0];
	}
}

sub id
{
	my ($tobj) = @_;
	my $obj = $tobj->_get_self();
	return $obj->{'id'};
}

1;

