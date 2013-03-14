package RevelDB::Storage::Hash;

use strict;
use warnings;

use base 'RevelDB::Storage';
use Data::Dumper;
use RevelDB::Util;

sub key_separator { ':H:' }
sub type          { 'hash' }

my %fields = (
    hset    => {
        arguments => 3,
        type      => 'hash',
    },
    hget    => {
        arguments => 2,
        type      => 'hash',
    },
    hdel    => {
        arguments => 2,
        type      => 'hash',
    },
    hexists => {
        arguments => 2,
        type      => 'hash',
    },
    hgetall => {
        arguments => 1,
        type      => 'hash',
    },
    hash_delete_key => {
        arguments => 1,
        type      => 'hash',
    },
    hkeys => {
        arguments => 1,
        type      => 'hash',
    },
    hvals => {
        arguments => 1,
        type      => 'hash',    
    },
    hmset => {
        arguments  => 3,
        type       => 'hash',
    },
    hmget => {
        arguments  => 2,
        type       => 'hash',
    },
    hlen => {
        arguments  => 1,
        type       => 'hash',
    }
);
sub get_fields {
    return \%fields;
}

# HSET key field value
sub redis_hset {
    my $self    = shift;
    my @data    = @_;

    my $key     = $data[0];
    my $field   = $data[1];
    my $value   = $data[2];

    my $new_key = sprintf("%s%s%s", $key, key_separator, $field);

    $self->set_type($key, type);
    $self->{db}->Put( $new_key, $value );

    return '+OK';
}

# HGET key field
sub redis_hget {
    my $self    = shift;
    my @data    = @_;

    my $key     = $data[0];
    my $field   = $data[1];

    my $new_key = sprintf("%s%s%s", $key, key_separator, $field);

    my $value   = $self->{db}->Get( $new_key);

    return '$-1' if !defined $value;

    return ('$' . length($value), $value);
}

# HDEL key field [field ...]
sub redis_hdel {
    my ($self, @data) = @_;

    my $key     = $data[0];
    my $field   = $data[1];
    my $new_key = sprintf("%s%s%s", $key, key_separator, $field);

    $self->{db}->Delete( $new_key );

    return '+OK';
}

# HEXISTS key field
sub redis_hexists {
    my ($self, @data) = @_;

    my $key     = $data[0];
    my $field   = $data[1];
    my $new_key = sprintf("%s%s%s", $key, key_separator, $field);

    my $value = $self->{db}->Get($new_key);

    return ( $value ) ? '+OK' : ();
}

# HGETALL key
sub redis_hgetall {
    my ($self, @data) = @_;

    my $key     = $data[0];
    my $new_key = $key . key_separator;
    my %hash    = %{$self->get_all($new_key)};

    my @resp;
    push( @resp, '*' . ( 2*scalar(keys %hash) ) );
    while ( my ($field, $value) = each(%hash) ) {
        push( @resp, '$' . length($field), $field );
        push( @resp, '$' . length($value), $value );
    }
    undef %hash;
    return @resp;
}

# HKEYS key
sub redis_hkeys {
    my ($self, $key) = @_;

    my $new_key = $key . key_separator;
    my %hash    = %{$self->get_all($new_key)};
    my @keys    = keys %hash;
    # print Dumper \@keys;
    return RevelDB::Util::return_array(@keys);
}

# HVALS key
sub redis_hvals {
    my ($self, $key) = @_;

    my $new_key = $key . key_separator;
    my %hash    = %{$self->get_all($new_key)};

    return RevelDB::Util::return_array(values %hash);
}

# HMSET key field value [field value ...]

sub redis_hmset {
    my $self = shift;
    my @data = @_;

    if ( scalar @data == 3 ) {
        my $key   = $data[0];
        my $field = $data[1];
        my $value = $data[2];
        # print Dumper \@data;
        my $new_key = $key . key_separator() . $field;
        # my $value   = $score . value_separator . $member;

        $self->set_type($key, type());
        my $is_new  = $self->{db}->Get( $new_key ) ? 0 : 1;
        my $res     = $self->{db}->Put( $new_key, $value );

        return RevelDB::Util::return_int($is_new);
    } else {
        my $key = shift @data;
        # local $" = " ";
        # print "HMSET: $key @data\n";
        $self->set_type($key, type());

        my $batch = Tie::LevelDB::WriteBatch->new();
        my $cnt   = scalar @data;
        while (@data) {
            my $field = shift @data;
            my $value = shift @data;
            my $new_key = $key . key_separator . $field;
            # print "Put($new_key, $value)\n";
            $batch->Put( $new_key, $value );
        }
        # print Dumper $batch;
        $self->{db}->Write($batch);
        undef $batch;
        return RevelDB::Util::return_int($cnt/2);
    }
}

# HLEN key
# Returns the number of fields contained in the hash stored at key
sub redis_hlen {
    my $self = shift;
    my $key  = shift;

    my $new_key = $key . key_separator;
    my $it      = $self->{db}->NewIterator;
    my $card    = 0;
    for ( $it->Seek($new_key); $it->Valid && index( $it->key, $new_key ) == 0; $it->Next ) {
        $card++;
    }
    undef $it;
    return RevelDB::Util::return_int($card);
}

sub redis_hmget {
    my $self = shift;
    my @data = @_;
    my $key  = shift @data;

    # my $it      = $self->{db}->NewIterator;
    # my $new_key = $key . key_separator;

    my @values;
    foreach my $field (@data) {
        my $new_key = $key . key_separator() . $field;
        push @values, $self->{db}->Get($new_key);
    }
    return RevelDB::Util::return_array(@values);
}

1;