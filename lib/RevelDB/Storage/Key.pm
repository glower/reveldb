package RevelDB::Storage::Key;

use strict;
use warnings;

use base 'RevelDB::Storage';

sub separator  {}

my %key_separator = (
    hash   => ':H:',
    zset   => ':Z:',
    list   => ':L:',
    set    => ':S:',
    string => '',
);

my %fields = (
    del => {
        arguments => 1,
        type      => '*',
    },
    exists => {
        arguments => 1,
        type      => '*',
    },
);
sub get_fields { return \%fields };

# TODO: delete all keys, if entry was hash or set or ...
sub redis_del {
    my $self = shift;
    my $key  = shift;

    my $type = $self->get_type($key);
	if ($type eq 'string') {
    	$self->{db}->Delete($key);		
	} else {
        my $separator = $key_separator{$type};
        return '$-1' if !$separator;
        my $new_key   = $key . $separator;
        # warn "[DEL] delete_key: $new_key*\n";
        my $it    = $self->{db}->NewIterator;
        my $batch = Tie::LevelDB::WriteBatch->new();
        for ( $it->Seek($new_key);
                $it->Valid && index( $it->key, $new_key ) == 0;
                $it->Next ) {
            # print "Delete(".$it->key.")\n";
            $batch->Delete($it->key);
        }
        $self->{db}->Write($batch);
        undef $it;
        undef $batch;
	}

    return "+OK";
}

sub redis_exists {
	my $self  = shift;
    my $key   = shift;

    my $value = $self->{db}->Get($key);
    return ( $value ) ? "+OK" : ();
}

1;