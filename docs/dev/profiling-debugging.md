# Profiling and debugging

## Profiling

On linux, restate-server exposes a heap profile endpoint, however profiling is not activated by default.
You can start the server with `MALLOC_CONF=prof_active:true` to activate it from startup,
or run `curl -XPUT localhost:5122/debug/pprof/heap/activate` to start profiling at runtime.

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

## Dumping tokio tasks
A backtrace of all tokio tasks can be obtained with `kill -usr2 <restate-pid>`.
If in a release docker image, it is strongly recommended to first run `download-restate-debug-symbols.sh`, the backtraces should pick up the debug symbols and be much more useful.
