use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Redis;
use Data::Dumper;
use Benchmark ':hireswallclock';

## Use UNIX domain socket
my $r = Redis->new(
	sock 	  => '/tmp/logdb.sock',
	debug     => 0,
);

# This function generates random strings of a given length
sub generate_random_string {
	my $length_of_randomstring = shift;

	my @chars=('a'..'z','A'..'Z','0'..'9','_','/');
	my $random_string;
	foreach (1..$length_of_randomstring) {
		$random_string.=$chars[rand @chars];
	}
	return $random_string;
}

my ($t0, $t1, $td, $rid);
my @rids = (50000..51000);

# my $str = generate_random_string(20+int rand 50);
# my $rid = $rids[rand int scalar @rids];
# $t0 = Benchmark->new;
# foreach my $x (1..10000) {
# 	$r->zadd("rid:$rid", $x, "/faaaoo/baaaar/mypic$x.jpg" );
# }
# $t1 = Benchmark->new;

# my $td = timediff($t1, $t0);
# print "the code took: ",timestr($td),"\n";

while (1) {
	$rid = $rids[rand int scalar @rids];
	my @data;
	# my $t = int rand 10;
	foreach my $x (1..10) {
		push @data, ("$x:".$x*12345, "/foaaao/baaaar/mypic$x.jpg" );
	}
	$t0 = Benchmark->new;
	$r->zadd("rid:$rid", @data );
	$t1 = Benchmark->new;

	$td = timediff($t1, $t0);
	print "the code took: ",timestr($td),"\n";
}