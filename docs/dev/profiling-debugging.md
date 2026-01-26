# Profiling and debugging

## Profiling

On linux, restate-server exposes a heap profile endpoint, however profiling is not activated by default.
You can start the server with `MALLOC_CONF=prof_active:true` to activate it from startup,
or run `curl -XPUT localhost:5122/debug/pprof/heap/activate` to start profiling at runtime.

From v1.6.0 onwards, the heap profile endpoint is at `/debug/heap/dump` and the `pprof` prefix is considered
deprecated. (`/debug/pprof/heap/activate` becomes `/debug/heap/activate`, and etc.)

You can then get a heap profile with `curl localhost:5122/debug/pprof/heap > heap.pb.gz` and evaluate it with `pprof heap.pb.gz` (`go install github.com/google/pprof@latest`).
Note the build ID, for release docker images you can obtain the symbols with
`curl -sL https://4bafe29d-dd3e-4c65-ba1d-54be833b2b69.debuginfod.polarsignals.com/buildid/$BUILD_ID/debuginfo -o restate-server`
and run `pprof ./restate-server heap.pb.gz`.

## Debugging

When using GDB on a process that comes from a release docker image you can set
`DEBUGINFOD_URLS=https://4bafe29d-dd3e-4c65-ba1d-54be833b2b69.debuginfod.polarsignals.com,https://debuginfod.elfutils.org/`.
For other tools, obtain the debug symbols by finding the build ID with `readelf -n restate-server` and then get the symbols with
`curl -sL https://4bafe29d-dd3e-4c65-ba1d-54be833b2b69.debuginfod.polarsignals.com/buildid/$BUILD_ID/debuginfo -o restate-server`.

Alternatively, inside release docker images, debug symbols can be downloaded (to `/usr/local/bin/.debug/restate-server.debug`) using `download-restate-debug-symbols.sh`.

## jemalloc Memory Statistics

Restate exposes jemalloc memory statistics via Prometheus metrics at `/metrics`:
- `restate_jemalloc_allocated_bytes` - Total bytes allocated by the application
- `restate_jemalloc_active_bytes` - Total bytes in active pages
- `restate_jemalloc_resident_bytes` - Physically resident memory
- `restate_jemalloc_retained_bytes` - Memory retained by jemalloc (not returned to OS)
- `restate_jemalloc_mapped_bytes` - Total mapped memory
- `restate_jemalloc_metadata_bytes` - jemalloc internal metadata

The gap between `allocated` and `resident` indicates memory held by jemalloc for performance optimization.

### Manual Memory Purge

To force jemalloc to release retained memory back to the OS:
```bash
curl -X POST http://localhost:9070/debug/jemalloc/purge
```

**Warning**: This is an expensive operation that can cause significant latency spikes. It blocks allocations during the purge and should only be used for debugging or one-off memory reclamation. Do not use this regularly in production.

## Dumping tokio tasks
A backtrace of all tokio tasks can be obtained with `kill -usr2 <restate-pid>`.
If in a release docker image, it is strongly recommended to first run `download-restate-debug-symbols.sh`, the backtraces should pick up the debug symbols and be much more useful.
