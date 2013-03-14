package RevelDB::Storage::SortedSet;

use strict;
use warnings;

use base 'RevelDB::Storage';
use RevelDB::Util;
use Digest::MD5 qw(md5 md5_hex);
use Data::Dumper;

sub key_separator   { ':Z:' }
sub value_separator { ':::' }
sub type            { 'zset' }

my %fields = (
    zadd    => {
        arguments => 3,
        type      => 'zset',
    },
    zrangebyscore => {
        arguments => 3,
        type      => 'zset',
    },
    zrange => {
        arguments => 3,
        type      => 'zset',
    },
    zcard         => {
        arguments => 1,
        type      => 'zset',
    },
    zscore        => {
        arguments => 2,
        type      => 'zset',
    },
    zrank        => {
        arguments => 2,
        type      => 'zset',
    },
    zincrby        => {
        arguments => 2,
        type      => 'zset',
    },
);

sub get_fields {
    return \%fields;
}

# ZADD key score member [score] [member]
# Adds all the specified members with the specified scores to the sorted set stored at key.
# It is possible to specify multiple score/member pairs. If a specified member is already
# a member of the sorted set, the score is updated and the element reinserted at the
# right position to ensure the correct ordering. If key does not exist, a new sorted
# set with the specified members as sole members is created, like if the sorted set was empty.
# If the key exists but does not hold a sorted set, an error is returned.
sub redis_zadd {
    my $self = shift;
    my @data = @_;
    # print "redis_zadd".Dumper \@data;
    if ( scalar @data == 3 ) {
        my $key     = $data[0];
        my $score   = $data[1];
        my $member  = $data[2];

        # warn "ZADD $key $score $member\n";

        my $new_key = $key . key_separator . md5_hex($member);
        my $value   = $score . value_separator . $member;

        $self->set_type($key, type());
        my $is_new  = $self->{db}->Get( $new_key ) ? 0 : 1;
        my $res     = $self->{db}->Put( $new_key, $value );

        return RevelDB::Util::return_int($is_new);
    } else {
        my $key = shift @data;
        $self->set_type($key, type());

        my $batch = Tie::LevelDB::WriteBatch->new();
        while (@data) {
            my $score   = shift @data;
            my $member  = shift @data;
            my $new_key = $key . key_separator . md5_hex($member);
            my $value   = $score . value_separator . $member;
            $batch->Put( $new_key, $value );
        }
        # print Dumper $batch;
        $self->{db}->Write($batch);
        undef $batch;
        return RevelDB::Util::return_int(scalar @data);
    }
}

# ZSCORE key member
# Returns the score of member in the sorted set at key.z
# If member does not exist in the sorted set, or key does not exist, nil is returned.
sub redis_zscore {
    my $self   = shift;
    my $key    = shift;
    my $member = shift;

    # warn "ZSCORE $key $member\n";
    my $new_key = $key . key_separator . md5_hex($member);
    my $value   = $self->{db}->Get( $new_key );
    # warn "[ZSCORE] value=$value\n";
    return "-1" if !$value;
    my ($score) = split( value_separator(), $value );
    return RevelDB::Util::return_int($score);
}


# ZINCRBY key increment member
# Increments the score of member in the sorted set stored at key by increment.
sub redis_zincrby {
    my $self      = shift;
    my $key       = shift;
    my $increment = shift || 0;
    my $member    = shift;

    # warn "ZINCRBY $key $increment $member\n";
    my $new_key     = $key . key_separator() . md5_hex($member);
    my $old_value   = $self->{db}->Get( $new_key );
    # warn "[ZINCRBY] Get( $new_key ) = $old_value\n";
    my ($old_score) = ($old_value) ? split( value_separator(), $old_value ) : 0;
    my $new_score   = $old_score + $increment;
    my $new_value   = $new_score . value_separator() . $member;
    $self->{db}->Put( $new_key, $new_value );
    # warn "[ZINCRBY] Put( $new_key, $new_value )\n";
    return RevelDB::Util::return_int($new_score);
}

# ZRANK key member
# Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
# The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
sub redis_zrank {
    my $self   = shift;
    my $key    = shift;
    my $member = shift;

    my $new_key = $key . key_separator();
    my $it      = $self->{db}->NewIterator;
    my $card    = 0;
    for ( $it->Seek($new_key); $it->Valid && index( $it->key, $new_key ) == 0; $it->Next ) {
        my ( $score, $this_member ) = split( value_separator(), $it->value );
        last if ($this_member eq $member);
        $card++;
    }
    undef $it;
    return RevelDB::Util::return_int($card);
}


# ZCARD key
# Returns the sorted set cardinality
# (number of elements) of the sorted set stored at key.
sub redis_zcard {
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


# ZRANGEBYSCORE key min max [WITHSCORES]
# Returns all the elements in the sorted set at key with a score between min and max
# (including elements with score equal to min or max). The elements are considered to be
# ordered from low to high scores.
sub redis_zrangebyscore {
    my $self = shift;
    my @data = @_;

    my $key        = $data[0];
    my $score_min  = $data[1];
    my $score_max  = $data[2];
    my $withscores = ($data[3] && $data[3] eq 'WITHSCORES');

    # warn "ZRANGEBYSCORE $key $score_min $score_max\n";
    # $score_max = time if !$score_max;

    my $it      = $self->{db}->NewIterator;
    my $new_key = $key . key_separator;

    my %members;
    for ( $it->Seek($new_key); $it->Valid && index( $it->key, $new_key ) == 0; $it->Next ) {
        my ( $score, $member ) = split( value_separator(), $it->value );
        next if !$member;
        if ( $score <= $score_max && $score >= $score_min ) {
            $members{$member} = $score;
        }
    }
    undef $it;

    my @members;
    foreach my $m ( sort { $members{$a} <=> $members{$b} } keys %members ) {
        push @members, $m;
        push @members, $members{$m} if $withscores;
    }
    undef %members;
    return RevelDB::Util::return_array(@members);
}

# ZRANGE key start stop [WITHSCORES]
# Returns the specified range of elements in the sorted set stored at key.
# The elements are considered to be ordered from the lowest to the highest score.
sub redis_zrange {
    my $self = shift;
    my @data = @_;

    my $key   = $data[0];
    my $start = $data[1];
    my $stop  = $data[2];
    my $withscores = ($data[3] && $data[3] eq 'WITHSCORES');

    my $it      = $self->{db}->NewIterator;
    my $new_key = $key . key_separator;

    my %members;
    for ( $it->Seek($new_key); $it->Valid && index( $it->key, $new_key ) == 0; $it->Next ) {
        my ( $score, $member ) = split( value_separator, $it->value );
        next if !$member;
        $members{$member} = $score;
        # push @members, $score if $withscores;
    }
    undef $it;

    my @members;
    foreach my $m ( sort { $members{$a} <=> $members{$b} } keys %members ) {
        push @members, $m;
        push @members, $members{$m} if $withscores;
    }
    my @range = @members[$start..$stop];
    undef %members;
    undef @members;
    return RevelDB::Util::return_array(@range);
}

sub redis_zset_delete_key {
    my ($self, $key) = @_;
    my $new_key = $key . key_separator;

    warn "[DEL] redis_hash_delete_key: $new_key\n";

    my $it      = $self->{db}->NewIterator;

    for ( $it->Seek($new_key);
          $it->Valid && index( $it->key, $new_key ) == 0;
          $it->Next ) {
        $self->{db}->Delete($it->key);
    }

    undef $it;
}

1;