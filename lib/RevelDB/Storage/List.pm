package RevelDB::Storage::List;

use strict;
use warnings;

use base 'RevelDB::Storage';

sub separator { ':L:' }
sub _type { 'list' }
sub get_fields {};

1;