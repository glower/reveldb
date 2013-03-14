#!/usr/bin/perl -w

use strict;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Test::More tests => 12;

BEGIN {
    use_ok('RevelDB::Server::Client');
    use_ok('RevelDB::Server::Listener');
    use_ok('RevelDB::Server');
    use_ok('RevelDB::Storage::Misc');
    use_ok('RevelDB::Storage::List');
    use_ok('RevelDB::Storage::String');
    use_ok('RevelDB::Storage::Key');
    use_ok('RevelDB::Storage::Hash');
    use_ok('RevelDB::Storage::SortedSet');
    use_ok('RevelDB::Storage::Set');
    use_ok('RevelDB::Storage');
    use_ok('RevelDB::Util');
}

diag("Testing RevelDB $RevelDB::Server::VERSION");