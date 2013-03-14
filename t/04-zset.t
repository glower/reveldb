#!perl

use warnings;
use strict;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Test::More tests => 49;
use Test::Deep;
use Test::Exception;
use Redis;
use Try::Tiny;
use Data::Dumper;

my $o = Redis->new(
  sock    => '/tmp/logdb.sock',
  # sock    => '/tmp/redis.sock',
  debug   => 0,
);

ok($o, 'connected to our test redis-server');
is($o->ping, 'PONG', 'ping-pong');

my $key = 'foo'.int rand 1000000000;
my @exp;
my $member1 = generate_random_string(10);
push @exp, $member1;
my $member2 = generate_random_string(10);
push @exp, $member2;
my $member3 = generate_random_string(10);
push @exp, $member3;
my $member4 = generate_random_string(15);

is( $o->zadd($key, 100, $member1), 1, "zadd $key 100 $member1");
is( $o->zadd($key, 101, $member2), 1, "zadd $key 101 $member2");
is( $o->zadd($key, 102, $member3), 1, "zadd $key 102 $member3");

my @d = $o->zrangebyscore($key, 0, 200);
@d   = sort @d;
@exp = sort @exp;
cmp_deeply(\@d, \@exp, "zrangebyscore $key 0 200");

my %exp = (
	$member1 => 100,
	$member2 => 101,
	$member3 => 102,
);

my %d = $o->zrangebyscore($key, 0, 200, "WITHSCORES");
cmp_deeply(\%d, \%exp, "zrangebyscore 0 200 WITHSCORES");

is($o->zcard($key), 3, "zcard($key) is 3");

%d = $o->zrangebyscore($key, 0, 101, "WITHSCORES");
cmp_deeply(\%d, {$member1 => 100, $member2 => 101}, "zrangebyscore 0 101 WITHSCORES");

%d = $o->zrangebyscore($key, 101, 102, "WITHSCORES");
cmp_deeply(\%d, {$member2 => 101, $member3 => 102}, "zrangebyscore 101 102 WITHSCORES");

is($o->zadd($key, 200, $member1), 0, "zadd $key 200 $member1");
is($o->zadd($key, 300, $member3), 0, "zadd $key 300 $member3");

%exp = (
	$member1 => 200,
	$member2 => 101,
	$member3 => 300,
);
%d = $o->zrangebyscore($key, 0, 500, "WITHSCORES");
cmp_deeply(\%d, \%exp, "zrangebyscore 0 500 WITHSCORES");

is($o->zadd($key, 800, $member2), 0, "zadd $key 800 $member2");

%exp = (
	$member1 => 200,
	$member2 => 800,
	$member3 => 300,
);
%d = $o->zrangebyscore($key, 0, 1000, "WITHSCORES");
cmp_deeply(\%d, \%exp, "zrangebyscore 0 1000 WITHSCORES");
$o->del($key);
%d = $o->zrangebyscore($key, 0, 1000, "WITHSCORES");
cmp_deeply(\%d, {}, "zrangebyscore: empty data for <$key>");

my @data = (
	100, 'alice',
	200, 'bob',
	300, 'charly',
);
%exp = (
	alice  => 100,
	bob    => 200,
	charly => 300,
);

$key = 'batch_key';
$o->del($key);

$o->zadd($key, @data);
%d = $o->zrangebyscore($key, 0, 1000, "WITHSCORES");
cmp_deeply(\%d, \%exp, "get data after batch insert for <$key>");
$o->del($key);
%d = $o->zrangebyscore($key, 0, 1000, "WITHSCORES");
cmp_deeply(\%d, {}, "zrangebyscore: empty data for <$key>");

undef @data;
undef %exp;
foreach my $x (1..5000) {
	push @data, ($x , "foo$x");
	$exp{"foo$x"} = $x;
}
$o->zadd($key, @data);

%d = $o->zrangebyscore($key, 0, 100000, "WITHSCORES");

cmp_deeply(\%d, \%exp, "get data after big batch insert for <$key>");

$o->del($key);
%d = $o->zrangebyscore($key, 0, 100000, "WITHSCORES");
cmp_deeply(\%d, {}, "get empty data after del($key)");

### TODO: fixme!!!
# ok $o->zadd($key, 0, $member4);
# %d = $o->zrangebyscore($key, 0, 1, "WITHSCORES");
# cmp_deeply(\%d, {$member4 => 0});

# ok $o->zadd($key, 11, '');
# %d = $o->zrangebyscore($key, 0, 1, "WITHSCORES");
# cmp_deeply(\%d, {$member4 => 0});


# ok $o->zadd($key, 0, '');
# %d = $o->zrangebyscore($key, 0, 1, "WITHSCORES");
# cmp_deeply(\%d, {$member4 => 0});

throws_ok { $o->zadd() } qr/.*wrong number of arguments for 'zadd' command.*/;
throws_ok { $o->zadd('foobar') } qr/.*wrong number of arguments for 'zadd' command.*/;
throws_ok { $o->zadd('foobar', 1) } qr/.*wrong number of arguments for 'zadd' command.*/;

throws_ok { $o->zrangebyscore() } qr/.*wrong number of arguments for 'zrangebyscore' command.*/;
throws_ok { $o->zrangebyscore('foobar') } qr/.*wrong number of arguments for 'zrangebyscore' command.*/;
throws_ok { $o->zrangebyscore('foobar', 1) } qr/.*wrong number of arguments for 'zrangebyscore' command.*/;

throws_ok { $o->get($key) } qr/.*Operation against a key holding the wrong kind of value.*/;

throws_ok { $o->hset($key, 'lorem' => 'ipsum') } qr/.*Operation against a key holding the wrong kind of value.*/;
throws_ok { $o->hget($key, 'foo') } qr/.*Operation against a key holding the wrong kind of value.*/;

sub generate_random_string {
	my $length_of_randomstring = shift;# the length of 

	my @chars=('a'..'z','A'..'Z','0'..'9','_','/', ':');
	my $random_string;
	foreach (1..$length_of_randomstring) {
		$random_string .= $chars[rand @chars];
	}

	return $random_string;
}

my $zset  = 'test-zset-'.int(rand(100));
$o->del($zset);

ok($o->zadd($zset, 0, 'foo'), "ZADD $zset 0 foo");
ok(!$o->zadd($zset, 3, 'foo'), "foo is already in the set"); # 0 returned because foo is already in the set

is($o->zscore($zset, 'foo'), 3, "ZSCORE $zset foo: 3");

is($o->zincrby($zset, 1, 'foo'), 4, "ZINCRBY $zset 1 foo: 4");
is($o->zscore($zset, 'foo'), 4, "ZSCORE $zset foo: 4");

is($o->zincrby($zset, 1, 'bar'), 1, "ZINCRBY $zset 1 bar: 1");
is($o->zscore($zset, 'bar'), 1, "ZSCORE $zset bar: 1"); # bar was new, so its score got set to the increment

is($o->zrank($zset, 'bar'), 0);
is($o->zrank($zset, 'foo'), 1);

#is($o->zrevrank($zset, 'bar'), 1);
#is($o->zrevrank($zset, 'foo'), 0);

ok($o->zadd($zset, 4.1, 'baz'), "ZADD $zset 2.1 baz"); # we now have bar foo baz
is_deeply([$o->zrange($zset, 0, 1)], [qw/bar foo/]);
# is_deeply([$o->zrevrange($zset, 0, 1)], [qw/baz foo/]);

$o->del($zset);
my $score = 0.1;
my @zkeys = (qw/foo bar baz qux quux quuux quuuux quuuuux/);

ok( $o->zadd($zset, $score++, $_), "ZADD: $_" ) for @zkeys;
is_deeply([$o->zrangebyscore($zset, 0, 8)], \@zkeys);

# is($o->zremrangebyrank($zset, 5, 8), 3);    # remove quux and up
# is_deeply([$o->zrangebyscore($zset, 0, 8)], [@zkeys[0 .. 4]]);

# is($o->zremrangebyscore($zset, 0, 2), 2);    # remove foo and bar
# is_deeply([$o->zrangebyscore($zset, 0, 8)], [@zkeys[2 .. 4]]);