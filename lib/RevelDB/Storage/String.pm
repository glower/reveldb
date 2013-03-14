package RevelDB::Storage::String;

use strict;
use warnings;

use base 'RevelDB::Storage';

use Data::Dumper;

sub _separator { '' }
sub _type      { 'string' };

my %fields = (
    get => {
        arguments => 1,
        type      => 'string',
    },
    set => {
        arguments => 2,
        type      => 'string',
    },
);
sub get_fields {
    return \%fields;
}
# GET key
sub redis_get {
    my RevelDB::Storage $self = shift;
    my $key = shift;

    my $value = $self->{db}->Get($key);
    # print "GET $key\n";
    return '$-1' if !defined $value;

    return ('$' . length($value), $value);
}

# SET key value
sub redis_set {
    my RevelDB::Storage $self = shift;
    my @data = @_;

    my $key   = $data[0];
    my $value = $data[1] || '';

    $self->set_type($key, _type);
    $self->{db}->Put( $key, $value );
	return "+OK";
}

# APPEND key value

# DECR key
# ERR value is not an integer or out of range

# DECRBY key decrement

# INCR key
# ERR value is not an integer or out of range

# INCRBY key decrement
# Atomically sets key to value and returns the old value stored at key.
# Returns an error when key exists but does not hold a string value.

1;