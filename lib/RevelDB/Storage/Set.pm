package RevelDB::Storage::Set;

use strict;
use warnings;

use base 'RevelDB::Storage';

sub separator { ':S:' }
sub type      { 'set' }

my %fields = (
    sadd => {
        arguments => 2,
        type      => 'set',
    },
    spop => {
        arguments => 1,
        type      => 'set',
    },
    smembers => {
        arguments => 1,
        type      => 'set',
    },
);

sub get_fields {
    return \%fields;
}

sub redis_sadd {
    # body...
}

# SADD key member [member ...]

# SINTER key [key ...]
# Returns the members of the set resulting from the intersection of all the given sets.

# SMOVE source destination member
# Move member from the set at source to the set at destination. 
# This operation is atomic. In every given moment the element will 
# appear to be a member of source or destination for other clients.

# SUNION key [key ...]
# Returns the members of the set resulting from the union of all the given sets.

# SMEMBERS key
# Returns all the members of the set value stored at key.

# SPOP key
# Removes and returns a random element from the set value stored at key.

# SRANDMEMBER key
# Return a random element from the set value stored at key.

# SREM key member [member ...]

1;