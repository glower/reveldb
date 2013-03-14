package RevelDB::Server;

use strict;
use warnings;

use Data::Dumper;
use IO::Socket;
use RevelDB::Server::Listener;
use RevelDB::Server::Client;
use RevelDB::Storage;
use Carp qw(croak);
use Sys::Hostname ();
use IO::Handle ();
use IO::Socket::INET;
use Tie::LevelDB;
use Socket qw(IPPROTO_TCP SOL_SOCKET SOCK_STREAM AF_UNIX SOCK_STREAM PF_UNSPEC);

our $VERSION = "0.64";

use fields (
    'path',
    'storage',
    'socket',
    'listeners',
);

sub new {
    my ($class, %opts) = @_;
    my $self = ref $class ? $class : fields::new($class);

    $self->debug(__PACKAGE__."->new()");

    $self->{socket} = {};
    my $socket = delete $opts{socket};
    my $port   = delete $opts{port};
    my $path   = delete $opts{path};

    $self->{storage} = RevelDB::Storage->new($path);
    croak("Unknown options") if %opts;
    $self->create_listening_sock(socket => $socket, port => $port);

    return $self;
}

sub debug {
    my ($self, $msg) = @_;
    # warn "DEBUG: $msg\n";
}

sub create_listening_sock {
    my ($self, %opts) = @_;

    my $sock = $opts{socket};
    my $port = $opts{port} || 0;

    my $ssock;
    if ($sock) {
        print "Server: start listening on socket $sock\n";
        unlink $sock;
        $ssock = IO::Socket::UNIX->new(
            Local  => $sock,
            Listen => 20,
        ) or die "Error creating socket: $@\n";
    }
    elsif ($port) {
        print "Server: start listening on port $port\n";
        $ssock = IO::Socket::INET->new(
            LocalPort => $port,
            Type      => SOCK_STREAM,
            Proto     => IPPROTO_TCP,
            Blocking  => 0,
            Reuse     => 1,
            Listen    => 1024 ) or die "Error creating socket: $@\n";
    } else {
        die "No socket specified\n";
    }

    my $listeners = $self->{listeners};
    push @$listeners, RevelDB::Server::Listener->new( $ssock, $self );

    return $ssock;
}

sub new_client {
    my ($self, $sock) = @_;

    my $client = RevelDB::Server::Client->new($sock, $self);
    $client->watch_read(1);
}

1;
__END__
