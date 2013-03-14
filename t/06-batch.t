#!/usr/bin/perl -w

use strict;
use Test::More tests => 8;

use Test::Deep;
use Redis;
use Data::Dumper;

my $o = Redis->new(
	sock    => '/tmp/logdb.sock',
  	# sock    => '/tmp/redis.sock',
  	debug   => 0,
);
my @data = ();
my %exp  = ();
my %d;

my $key  = "key:".int(rand 100000000);
$o->zadd($key, 1, 'a', 2, 'b', 3, 'c');
%d = $o->zrangebyscore($key, 0, 200, "WITHSCORES");
cmp_deeply(\%d, {a => 1, b => 2, c => 3}, "zrangebyscore 0 200 WITHSCORES");
$o->del($key);

%d = $o->zrangebyscore($key, 0, 200, "WITHSCORES");
cmp_deeply(\%d, {}, "zrangebyscore is empty");


@data = (100, 'alice', 200, 'bob');
%exp  = (alice => 100, bob => 200);

$o->zadd($key, @data);
%d = $o->zrangebyscore($key, 0, 200, "WITHSCORES");
cmp_deeply(\%d, \%exp, "zrangebyscore 0 200 WITHSCORES");
$o->del($key);

%d = $o->zrangebyscore($key, 0, 200, "WITHSCORES");
cmp_deeply(\%d, {}, "zrangebyscore is empty");

undef @data;
undef %exp;
foreach my $x (1..10000) {
	push @data, ($x , "member:$x");
	$exp{"member:$x"} = $x;
}
$o->zadd($key, @data);

%d = $o->zrangebyscore($key, 0, 1000000, "WITHSCORES");

cmp_deeply(\%d, \%exp, "get data after big batch insert");

$o->del($key);
%d = $o->zrangebyscore($key, 0, 1000, "WITHSCORES");
cmp_deeply(\%d, {}, "get empty data after del($key)");

undef @data;
undef %exp;
foreach my $x (1..100000) {
	my $score  = time-$x;
	my $member = generate_random_string(30+int rand 20);
	push @data, ( $score, $member );
	$exp{$member} = $score;
}
$o->zadd($key, @data);

%d = $o->zrangebyscore($key, 0, time, "WITHSCORES");
cmp_deeply(\%d, \%exp, "get data after big batch insert");

$o->del($key);
%d = $o->zrangebyscore($key, 0, time, "WITHSCORES");
cmp_deeply(\%d, {}, "get empty data after del($key)");

sub generate_random_string {
	my $length_of_randomstring = shift;

	my @chars=('a'..'z','A'..'Z','0'..'9','_','/', '.');
	my $random_string;
	foreach (1..$length_of_randomstring) {
		$random_string .= $chars[rand @chars];
	}

	return $random_string;
}