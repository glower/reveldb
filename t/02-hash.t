#!perl

use warnings;
use strict;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Test::More tests => 33;
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

# use_ok('RevelDB::Storage::Hash');
ok($o, 'connected to our test redis-server');
is($o->ping, 'PONG', 'ping-pong');

## Commands operating on hashes

my $hash = 'test-hash-'.int rand 1000000000;
$o->del($hash);

throws_ok { $o->hget($hash) } qr/.*wrong number of arguments for 'hget' command.*/, "HGET Exception OK";
throws_ok { $o->hget() } qr/.*wrong number of arguments for 'hget' command.*/, "HGET Exception OK";

ok($o->hset($hash, foo => 'bar'), "HSET ok");
is($o->hget($hash, 'foo'), 'bar', "HGET ok");

is($o->hget($hash, 'notfoo'), undef, "HGET undef ok");

throws_ok { $o->get($hash) } qr/.*Operation against a key holding the wrong kind of value.*/;
throws_ok { $o->hset($hash, 'lorem') } qr/.*wrong number of arguments for 'hset' command.*/;
throws_ok { $o->hset($hash) } qr/.*wrong number of arguments for 'hset' command.*/;
throws_ok { $o->hset() } qr/.*wrong number of arguments for 'hset' command.*/;

throws_ok { $o->hget($hash) } qr/.*wrong number of arguments for 'hget' command.*/;
throws_ok { $o->hget() } qr/.*wrong number of arguments for 'hget' command.*/;

$o->hset($hash, 'lorem' => 'ipsum');
$o->hset($hash, 'du' => 'da');
my %h = $o->hgetall($hash);

cmp_deeply(\%h, {lorem => 'ipsum', foo => 'bar', du => 'da'}, 'HGETALL OK');

ok($o->hset($hash, space => ''), "HSET '' ok");
cmp_ok($o->hget($hash, 'space'), 'eq','', "HGET '' ok");

ok($o->hexists($hash, 'foo'), "hexists ok");
ok($o->hdel($hash, 'foo'), 'hdel ok');
ok(!$o->hexists($hash, 'foo'), 'hexists after hdel');

$o->del($hash);
%h = $o->hgetall($hash);
cmp_deeply(\%h, {}, 'HGETALL empy hash');

undef %h;
my %exp;
$o->del('big_hash');
foreach my $x (1..10) {
	$o->hset('big_hash', "key:$x" => "value:$x");
	$exp{"key:$x"} = "value:$x";
}
%h = $o->hgetall('big_hash');
cmp_deeply(\%h, \%exp, 'HGETALL big_hash');

my @k    = sort $o->hkeys('big_hash');
my @keys = sort keys %exp;
cmp_deeply(\@k, \@keys, 'HKEYS big_hash');

my @v 	   = sort $o->hvals('big_hash');
my @values = sort values %exp;
cmp_deeply(\@v, \@values, 'HVALS big_hash');


$o->del('big_hash');
%h = $o->hgetall('big_hash');
cmp_deeply(\%h, {}, 'HGETALL empy big_hash');

# ok($o->hincrby($hash, incrtest => 1));
# is($o->hget($hash, 'incrtest'), 1);

# is($o->hincrby($hash, incrtest => -1), 0);
# is($o->hget($hash, 'incrtest'), 0);

# ok($o->hdel($hash, 'incrtest'));    #cleanup

# ok($o->hsetnx($hash, setnxtest => 'baz'));
# ok(!$o->hsetnx($hash, setnxtest => 'baz'));    # already exists, 0 returned

$o->del($hash); #cleanup
my %data = (foo => 1, bar => 2, baz => 3, qux => 4);
is($o->hmset($hash, %data), 4, "HMSET put 4 new elements");
is_deeply([$o->hmget($hash, qw/foo bar baz/)], [1, 2, 3], "HMGET");
### FixMe!
# is($o->hmset($hash, foo => 1, wot => 5), 1, "HMSET put 1 new element");
$o->del($hash); #cleanup

is($o->hmset($hash, aaa => 'bbb'), 1, "HMSET put 1 new element");
%data = (foo => 1, bar => 2, baz => 3, qux => 4);
is($o->hmset($hash, %data), 4, "HMSET put 4 new elements");
is($o->hlen($hash), 5);
is_deeply([sort $o->hkeys($hash)], [sort qw/foo bar baz qux aaa/], 'HKEYS');
is_deeply([sort $o->hvals($hash)], [sort qw/1 2 3 4 bbb/], 'HVALS');
is_deeply({$o->hgetall($hash)}, {foo => 1, bar => 2, baz => 3, qux => 4, aaa => 'bbb'}, "HGETALL");
ok($o->del($hash));                            # remove entire hash

