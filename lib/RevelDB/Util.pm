package RevelDB::Util;

use strict;
use warnings;
use Data::Dumper;

sub return_int {
    my $int = shift // 0;
    # warn "return_int($int)\n";
    my $val = ":$int";
    # warn "return_int() $val\n";
    return $val;
}

sub return_array {
	my @data = @_;
    # print Dumper \@data;
	my @resp;
    push( @resp, '*' . ( scalar(@data) ) );
    for my $entry ( @data ) {
        push( @resp, '$' . length($entry), $entry );
    }
    undef @data;
    return @resp;
}


1;
