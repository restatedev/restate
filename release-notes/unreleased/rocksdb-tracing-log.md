# Release Notes: Route RocksDB diagnostics through server logging

## Behavioral Change

### What Changed

RocksDB diagnostics now use the server's logging output under the `rocksdb`
target, with a `db` field identifying the database. RocksDB no longer writes
or rotates `LOG` files. Warnings, errors and fatal messages are enabled by
default, including RocksDB's write-stall and write-stop diagnostics.

### Why This Matters

Write stalls and stops are now visible in server logs by default, together
with their causes: immutable memtables waiting for flush, too many L0 files,
or excessive pending compaction bytes. These diagnostics can be collected
without collecting separate files from each database directory.

### Impact on Users

The `rocksdb-log-level`, `rocksdb-log-keep-file-num`, and
`rocksdb-log-max-file-size` settings are deprecated in v1.8.0. They remain
accepted for compatibility but have no effect, including per-store overrides.
Existing log files are not removed by this change.

### Migration Guidance

- Collect RocksDB diagnostics from server logging instead of database `LOG` files.
- Remove the deprecated settings from your configuration.
- Use `log-filter` or `RUST_LOG` to control verbosity. For example,
  `warn,restate=info,rocksdb=warn` enables write-stall and write-stop diagnostics
  and other warnings and errors; use `rocksdb=info` for more detail, `rocksdb=debug` to include
  multiline statistics dumps, or `rocksdb=trace` for RocksDB debug messages.
- Keep your other filter directives when changing the `rocksdb` directive.
  An existing custom filter replaces the default filter, so add
  `rocksdb=warn` explicitly to ensure write stalls and stops remain visible.
- Restart the server after changing the filter. The native logger's threshold
  is selected when each database is opened.

The current RocksDB callback reports startup headers as info messages, so
`rocksdb=info` also includes the startup options dump. These headers cannot
currently be filtered separately from other info messages.
