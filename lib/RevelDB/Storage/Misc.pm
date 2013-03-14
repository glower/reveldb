package RevelDB::Storage::Misc;

use strict;
use warnings;

use base 'RevelDB::Storage';
use RevelDB::Server;
use POSIX;
use Data::Dumper;

my %fields = (
	ping 	=> {
		arguments => 0,
		type      => '*',
	},
	info 	=> {
		arguments => 0,
		type      => '*',
	},
);

sub get_fields {
	return \%fields;
}

sub redis_ping {
    my $self = shift;

    return "+PONG";
}

sub redis_info {
	my $self = shift;

	my $ver = $RevelDB::Server::VERSION;
	my ($sysname, $nodename, $release, $version, $machine) = POSIX::uname();
	my $os = "$sysname $release $machine";
	# $os =~ s/\s//g;

	my @info = (
		"logdb_version:$ver",
		"os:$os",
	);
	# my $info;
	my $leng = 0;
	foreach my $line (@info) {
		$leng = $leng+length($line); 
    }
    unshift @info, '$' . $leng;
	# print Dumper \@info;
	return @info;
}

1;