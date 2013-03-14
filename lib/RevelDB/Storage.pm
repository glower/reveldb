package RevelDB::Storage;

# redis commands factory

use strict;
use warnings;

use Tie::LevelDB;
# use Tie::LevelDB::WriteBatch;
use Class::Inspector;
use Data::Dumper;
use Carp qw/confess/;

use Module::Find qw/useall/;
my @types = useall 'RevelDB::Storage';

my %command;
my %info;

foreach my $class (@types) {
    # my $obj = $type->new();

    my $fields = \&{$class."::get_fields"};
    my $info   = $fields->() || {};
    %info   = ( %info, %{ $info } );

    my @functions = grep /^redis_/, @{Class::Inspector->functions($class)};
    foreach my $function (@functions) {
    	my ($cmd) = $function =~ /^redis_(\w+)/;
	    $command{uc $cmd} = \&{$class."::".$function};
        # $fields
    }
}

# print Dumper \%info;

use fields (
    'path',
    'cache',
    'batch',
    'db',
);

sub new {
    my ($class, $path) = @_;
    my $self = ref $class ? $class : fields::new($class);

    return if !$path;
    $self->{db}    = Tie::LevelDB::DB->new($path);
    $self->{cache} = {};

    return $self;
}

sub get_all {
    my ($self, $key) = @_;

    my $it = $self->{db}->NewIterator;
    my %hash;
    for ( $it->Seek($key); $it->Valid && index( $it->key, $key ) == 0; $it->Next ) {
        my ( $field ) = $it->key =~ m/$key(.*)/;
        $hash{$field} = $it->value;
    }
    undef $it;
    return \%hash;
}

sub check_type {
    my ($self, $key, $type) = @_;

    confess "key not set" if !$key;
    confess "type not set" if !$type;

    my $value = $self->get_type($key);

    return -1 if !$value;
    # warn "check_type: type of [$key] is [$value], we expect [$type]\n";
    return $value eq $type;
}

sub get_type {
    my ($self, $key) = @_;

    confess "key not set" if !$key;

    my $value;
    if ( exists $self->{cache}{type}{"key:::$key"} ) {
        # warn "$key with type:$type is in cache\n";
        $value = $self->{cache}{type}{"key:::$key"};
    } else {
        $value = $self->{db}->Get( "key:::$key") || '';
        $self->{cache}{type}{"key:::$key"} = $value if $value;
    }

    return $value;
}

sub check_arguments {
    my $self = shift;
    my $cmd  = lc shift;
    my @data = @_;

    # return { error => 0 };

    my $error = sub {
        return { error => 1, message => shift };
    };

    return $error->($self->error_unknown_command($cmd)) if !$info{$cmd};

    my $nuber_of_agrs = $info{$cmd}{arguments};
    my $type          = $info{$cmd}{type};

    # check number of args
    if (scalar @data < $nuber_of_agrs) {
        return $error->( $self->error_wrong_arguments($cmd) );
    }

    return { error => 0 } if $type eq '*';
    my $key     = $data[0];
    my $check   = $self->check_type($key, $type);
    if (!$check) {
        return $error->( $self->error_wrong_type() );
    }

    return { error => 0 };
}

sub set_type {
    my ($self, $key, $type) = @_;

    confess "key not set" if !$key;
    confess "type not set" if !$type;

    # warn "set_type: $key=$type\n";
    if ( !exists $self->{cache}{type}{"key:::$key"} ) {
        $self->{cache}{type}{"key:::$key"} = $type;
        $self->{db}->Put( "key:::$key", $type);        
    } else {
        # warn "$key with type:$type is in cache\n";
    }
}

sub get_and_cache {
    my ($self, $key) = @_;

    my $value = $self->{cache}{keys}{$key};
    if ( !$value ) {
        $value = $self->{db}->Get($key);
        $self->{cache}{keys}{$key} = $value;
    }
    return $value;
}

sub set_and_cache {}

sub delete_type {
    my ($self, $key, $type) = @_;

    confess "key not set" if !$key;
    confess "type not set" if !$type;

    $self->{db}->Delete( "key:::$key", $type);
}

sub error_wrong_arguments {
    my ($self, $command) = @_;

    return "-ERR wrong number of arguments for '$command' command";
}

sub error_wrong_type {
    return "-ERR Operation against a key holding the wrong kind of value";
}

sub error_unknown_command {
    my ($self, $cmd) = @_;
    return "-ERR unknown command '$cmd'";
}

sub AUTOLOAD {
    my $self = shift;
    my @data = @_;
    my $cmd  = our $AUTOLOAD;
    $cmd     =~ s/.*://;

    if ( defined $command{$cmd} ) {
        my $arguments = $info{lc $cmd};
        my $check     = $self->check_arguments($cmd, @data);

        return [$check->{message}] if $check->{error};
        return [$command{$cmd}->($self, @data)];
    } else {
        return [$self->error_unknown_command($cmd)];
    }
}

1;