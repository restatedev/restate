# Partition storage resource cleanup

## Bug Fix

Fixed a resource leak that could keep local partition databases and their memory
allocated after the worker released them.

Fixed incorrect reporting of locally persisted partition progress when a partition
is recreated or restored from a snapshot while background writes are completing.
Pending progress updates are also preserved when a partition is closed and reopened.

Fixed a rare hang when configuration is reloaded while a local database is closing.

No configuration changes are required.
