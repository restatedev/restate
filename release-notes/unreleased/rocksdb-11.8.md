# Upgrade the storage engine to RocksDB 11.8

## Behavioral Change

Restate's embedded storage engine now uses RocksDB 11.8.1, up from 11.1.2. This
applies to the partition store, the log server, the local loglet, and the
metadata server.

Existing databases open unchanged and no migration is needed. Restate
configuration keys are unchanged.
