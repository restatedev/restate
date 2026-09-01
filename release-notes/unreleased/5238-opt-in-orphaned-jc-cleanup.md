# Release Notes for Issue #5238: Opt In to Orphaned Journal Index Cleanup

## Behavioral Change

### What Changed

The one-time cleanup of orphaned journal completion-id index entries no longer runs
automatically in the background. It is now disabled by default and can be enabled with:

```toml
experimental_enable_jc_orphan_cleanup = true
```

When enabled, each partition with a pending cleanup completes the scan before it starts
processing records. Fresh partition stores and stores that have already completed the cleanup
skip the scan.

### Why This Matters

Running cleanup before partition processing prevents it from racing with creation of a new journal
that reuses an invocation ID.

### Impact on Users

Opted-in partition startup can take longer while the cleanup scans the journal completion-id index.
The unavailability of partitions depends on the number of journal entries and how many orphaned journal
completion id index entries are present. An `INFO` event reports when the scan starts and when it 
completes, including elapsed duration and scan/deletion counters. Cleanup failure prevents that partition 
from starting.

### Migration Guidance

Set `experimental_enable_jc_orphan_cleanup = true` to run the cleanup. Successful completion is
persisted per partition. Cancellation or failure leaves the cleanup pending and it is retried on the
next opted-in startup.

### Related Issues

- Issue #5238
