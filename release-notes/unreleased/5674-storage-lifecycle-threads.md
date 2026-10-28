# Configure storage lifecycle concurrency

Database opening and cleanup can now proceed concurrently across databases.
Use `storage-lifecycle-threads` to configure this pool (default: 4 threads).

Use `worker.storage.lifecycle-threads` to configure the separate pool for
partition-store creation, snapshot import, and deletion (default: 4 threads).

Both settings must be positive integers and require a restart to take effect.
They are independent of the ordinary storage I/O thread pools.
