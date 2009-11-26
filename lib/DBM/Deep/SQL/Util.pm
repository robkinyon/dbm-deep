package DBM::Deep::SQL::Util;

use strict;
use warnings FATAL => 'all';

sub _create {
	my ($obj, $type, $data) = @_;
	if ($type eq 'hash' || $type eq 'array') {
		$obj->_insert(
			'table' => 'rec_item',
			'fields' => {
				'item_type' => $type,
			},
		);
		my $id = $obj->_lastid();
		$obj->_insert(
			'table' => 'rec_'. $type,
			'fields' => {
				'id' => $id,
			},
		);
		return $id;
	}
	else {
		$obj->_insert(
			'table' => 'rec_'. $type,
			'fields' => $data,
		);
		return $obj->_lastid();
	}
}

sub _lastid {
	my ($obj) = @_;
	my $sth = $obj->{'dbi'}->query('select last_insert_id()');
	my $q = $sth->fetchall_arrayref();
	return $q->[0]->[0];
}

sub _select {
	my ($obj, @arg) = @_;
	my %prm = @arg;
	my $sth = $obj->{'dbi'}->select(\%prm);
	return $sth->fetchall_arrayref();
}

sub _insert {
	my ($obj, @arg) = @_;
	my %prm = @arg;
	return $obj->{'dbi'}->insert(\%prm);
}

sub _update {
	my ($obj, @arg) = @_;
	my %prm = @arg;
	return $obj->{'dbi'}->update(\%prm);
}

sub _delete_sql {
	my ($obj, $table, $where) = @_;
	return $obj->{'dbi'}->delete($table, $where);
}

sub _clone_tree {
	my ($obj, $data) = @_;
	if (ref($data)) {
		if ($data =~ /HASH/) {
			my %nv = ();
			foreach my $k (keys %$data) {
				$nv{$k} = $obj->_clone_tree($data->{$k});
			}
			return \%nv;
		}
		elsif ($data =~ /ARRAY/) {
			my @nv = ();
			foreach my $i (0..$#{$data}) {
				$nv[$i] = $obj->_clone_tree($data->[$i]);
			}
			return \@nv;
		}
		elsif ($data =~ /SCALAR/) {
			my $nv = $obj->_clone_tree($$data);
			return \$nv;
		}
	}
	else {
		my $nv = $data;
		return $nv;
	}
}

sub _tiearray {
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

sub _tiehash {
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

1;
__END__
