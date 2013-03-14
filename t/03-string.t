#!perl

use warnings;
use strict;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Test::More tests => 28;
use Test::Deep;
use Test::Exception;
use Redis;
use Try::Tiny;
use Data::Dumper;
use Digest::MD5 qw(md5 md5_hex md5_base64);

my $o = Redis->new(
  sock    => '/tmp/logdb.sock',
  # sock    => '/tmp/redis.sock',
  debug   => 0,
);

ok($o, 'connected to our test redis-server');
is($o->ping, 'PONG', 'ping-pong');

my $key = 'foo'.int rand 1000000000;
$o->del($key);
ok( !$o->exists($key), "key $key dose not exists");

# ok(!$o->setnx(foo => 'bar'), 'setnx foo => bar fails');
ok($o->set(foo => ''), 'set key => ""');
cmp_ok($o->get('foo'), 'eq', '', 'get key = ""');

ok($o->set($key => 'baz'), 'set $key => baz');
cmp_ok($o->get($key), 'eq', 'baz', 'get foo = baz');
$o->del($key);

my $euro = "\x{20ac}";
ok($o->set(utf8 => $euro), 'set utf8');
cmp_ok($o->get('utf8'), 'eq', $euro, 'get utf8');
$o->del('utf8');

ok($o->set('test-undef' => 42), 'set test-undef');
ok($o->exists('test-undef'), 'exists undef');
$o->del('test-undef');

throws_ok { $o->set() } qr/.*wrong number of arguments for 'set' command.*/;
throws_ok { $o->set('foobar') } qr/.*wrong number of arguments for 'set' command.*/;

throws_ok { $o->get() } qr/.*wrong number of arguments for 'get' command.*/;

$o->set($key => 'baz');
throws_ok { $o->hget($key, 'foo') } qr/.*ERR Operation against a key holding the wrong kind of value.*/;
throws_ok { $o->zrangebyscore($key, 1, 2) } qr/.*ERR Operation against a key holding the wrong kind of value.*/;
throws_ok { $o->foobar($key) } qr/.*ERR unknown command 'FOOBAR'.*/;
$o->del($key);

### Big sized keys
for my $size (10 * 1024, 50 * 1024, 100 * 1024, 200 * 1024) {
	my $v = generate_random_string($size);
  	ok($o->set('big_key_'.$size, $v), "set with value size $size ok");
  	is(length $o->get('big_key_'.$size), length $v, "... and get was ok to");
	$o->del('big_key_'.$size);
}

sub generate_random_string {
	my $length_of_randomstring = shift;# the length of 

	my @chars=('a'..'z','A'..'Z','0'..'9','_','/', ':', '+', '*');
	my $random_string;
	foreach (1..$length_of_randomstring) {
		$random_string .= $chars[rand @chars];
	}
	return $random_string;
}

$o->del('foo');
$o->set('foo' => 'bar');
is( $o->get('foo'), 'bar', 'foo=>bar' );
$o->set('foo' => 'baz');
is( $o->get('foo'), 'baz', 'foo=>baz' );
$o->set('foo' => 'xxx');
is( $o->get('foo'), 'xxx', 'foo=>xxx' );