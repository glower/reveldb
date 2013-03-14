package RevelDB::Server::Client;

use strict;

use Danga::Socket;
use base 'Danga::Socket';

use Carp;
use POSIX;
use Digest::MD5 qw(md5_hex);
use Data::Dumper;

use fields (
    'storage',
    'server',
    'options',
    'read_buf',
);

# 60k read buffer default, similar to perlbal's backend read.
use constant READ_SIZE     => 64 * 1024;
use constant MAX_READ_SIZE => 10000 * 1024;

my $debug = 0;

# Class Method:
sub new {
    my RevelDB::Server::Client $self = shift;
    my ($sock, $server) = @_;

    $server->debug(__PACKAGE__."->new()");

    $self = fields::new($self) unless ref $self;
    $self->SUPER::new($sock);

    $self->{server}      = $server;
    $self->{storage}     = $server->{storage}; 
    $self->{options}     = {};
    $self->{read_buf}    = '';

    return $self;
}

sub option {
    my RevelDB::Server::Client $self = shift;
    my $option = shift;

    return $self->{options}->{$option};
}

sub close {
    my RevelDB::Server::Client $self = shift;

    $self->SUPER::close;
}

# Client
sub event_read {
    my RevelDB::Server::Client $self = shift;

    my $bref = $self->read(READ_SIZE);

    return $self->write(sub { $self->close } ) unless defined $bref;

    if ( length($$bref) == READ_SIZE ) {
        my $limit = int(MAX_READ_SIZE / READ_SIZE);
        my @crefs = ($$bref);
        my $lng   = 0;
        while (my $cref = $self->read(READ_SIZE)) {
            push(@crefs, $$cref);
            $lng = $lng + length($$cref);
            last if (length($$cref) < READ_SIZE || $limit-- < 1);
        }
        $bref = \join('', @crefs);
        undef @crefs;
    }
    # print "data length is ".length($$bref)."\n";
    $self->{read_buf} .= $$bref;

    my @data = split '\r\n', $self->{read_buf};

    my $next_length;
    # *<number of arguments> CR LF
    my $elements = shift @data;
    my $type     = substr( $elements, 0, 1 ) || '';

    if ($type eq '*') {
        $elements = substr($elements, 1);
        if ($elements !~ m/^\d+$/) {
            warn "\n[ERROR] wrong package, first line is not int!\n";
            warn "$elements\n";
            print Dumper \@data;
            $self->write("\$-1\r\n");
            # die;
        }
        my $exp = $elements*2;
        my $rec = scalar(@data);

        if ($exp == $rec) {
            # print "[OK] expected: $exp, got: $rec\n";
            $self->{read_buf} = '';
        } else {
            # we have some more data to read, keep read_buffer
            # warn "[ERROR] wrong elements number, expected: $exp, got: $rec\n";
            # print  $self->{read_buf};
            my $last_element = $data[-1];
            # if last element is an empty string, we have no element for this in the data array
            if ($last_element ne '$0') {
                undef @data;
                @data = [];
                return;
            } else {
                # print "[OK] last element was empty\n";
                $self->{read_buf} = '';
            }
        }
    } else {
        warn "[ERROR] broken data\n";
        print $self->{read_buf}; 
        $self->{read_buf} = '';
        undef @data;
        @data = [];
        return;
    }

    if (!@data) {
        warn "[ERROR] no data\n";
        print $self->{read_buf}; 
        $self->{read_buf} = '';
        undef @data;
        @data = [];
        $self->write("\$-1\r\n");
        return;
    }

    my @work;

    while ( $elements > 0 ) {
        my $line = shift( @data );
        my $type = substr( $line, 0, 1 ) || '';

        if ($type eq '$' && !defined $next_length) {
            $next_length = substr($line, 1);
            next;
        }
        elsif (defined $next_length) {
            my $data = substr($line, 0, $next_length);
            $data = '' if !defined $data;
            push @work, $data;
            undef $next_length;
            $elements--;
        } else {
            warn "[ERROR] $elements elements left\n";
            $self->{read_buf} = '';
            $self->write("\$-1\r\n");
            return;
        }
    }
    undef @data;

    return if !@work;
    my @result = $self->process(@work);
    $self->write("\$-1\r\n") if !@result;

    foreach my $line (@result) {
        # print "$line\r\n";
        $self->write("$line\r\n");
    }
}

sub event_write {
    my $self = shift;
    my $done = $self->write(undef);
    $self->watch_write(0) if $done;
}

sub process {
    my $self = shift;
    my @data = @_;

    my $storage = $self->{storage};
    my $cmd     = uc shift @data;
    if (!$cmd) {
        return ();
    }
    my $result  = $storage->$cmd( @data );
    return @$result;
}

1;
