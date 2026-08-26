# Release Notes for Issue #5235: Disable tokio task dumping (SIGUSR2)

## Behavioral Change

### What Changed
Sending `SIGUSR2` to `restate-server` no longer dumps tokio task backtraces to stderr. Instead, the
server logs a warning explaining that task dumping is not available. The `taskdump` feature is no
longer enabled in the shipped binaries.

### Why This Matters
Tokio's task dump collects backtraces by re-polling live task futures. Re-polling is not
side-effect free: a future that wakes a task from within `poll` can re-enter the scheduler it is
currently being polled by. On Restate's `current_thread` runtimes this can panic and crash the
process.

### Impact on Users
- `SIGUSR2` is now a no-op apart from a warning in the log. `SIGUSR1` (configuration dump) and
  `SIGHUP` (RocksDB flush + compaction) are unaffected.
- Nodes can no longer be aborted by sending `SIGUSR2`.
- No configuration change is required, and no other functionality is affected.

### Migration Guidance
None required. If you previously relied on `SIGUSR2` to investigate stuck tasks, use the metrics,
tracing, and log output instead, or reach out to us so we can help with the specific investigation.

### Related Issues
- Issue #5235: SIGUSR2 task dump can abort a server on `current_thread` runtimes
