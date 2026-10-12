# Avoid delays to other partitions during shutdown

## Bug Fix

Stopping a partition processor no longer blocks unrelated partitions from starting
or stopping while its background work is cleaned up. No configuration changes are
required.
