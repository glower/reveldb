use warnings;
use strict;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Test::More tests => 3;
use Test::Deep;
use Data::Dumper;
use Redis;
use RevelDB::Server;

my $r = Redis->new(
	sock => '/tmp/logdb.sock',
);

ok($r, 'connected to our test redis-server');
is($r->ping, 'PONG', 'ping-pong');

my $version = $RevelDB::Server::VERSION;

my $info = $r->info();
is( $info->{logdb_version}, $version, "logdb_version is $version" );