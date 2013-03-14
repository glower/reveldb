package RevelDB::Server::Listener;

use strict;
use base 'Danga::Socket';
use fields qw(server);

use Errno qw(EAGAIN);
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET SO_ERROR);


sub new {
    my RevelDB::Server::Listener $self = shift;
    my $sock   = shift;
    my $server = shift;

    $server->debug(__PACKAGE__."->new()");

    $self = fields::new($self) unless ref $self;

    # make sure provided listening socket is non-blocking
    IO::Handle::blocking($sock, 0);

    $self->SUPER::new($sock);

    $self->{server} = $server;

    $self->watch_read(1);

    return $self;
}

sub event_read {
    my RevelDB::Server::Listener $self = shift;

    my $listen_sock = $self->sock;
    print "Server: new server connection\n";

    while (my $csock = $listen_sock->accept) {
        IO::Handle::blocking($csock, 0);

        my $server = $self->{server};
        $server->new_client($csock);
    }

    return if $! == EAGAIN;

    warn "Error accepting incoming connection: $!\n";

    $self->watch_read(0);

    Danga::Socket->AddTimer( .1, sub {
        $self->watch_read(1);
    });
}


1;
