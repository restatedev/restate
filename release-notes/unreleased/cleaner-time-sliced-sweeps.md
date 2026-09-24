# Release Notes: Spread invocation cleanup over the cleanup interval

## Behavioral Change

### What Changed

`worker.cleanup-interval` now means the time within which the cleaner completes one full sweep
of a partition's invocation status table. Before, the cleaner scanned the whole partition once
per interval.

The cleaner splits the interval into slices of about 5 minutes, capped at 1000 slices. Each slice
scans a disjoint part of the partition's key range. With the default 1 hour interval, the cleaner
runs 12 scans per partition, one every 5 minutes, and each covers 1/12 of the key range.

### Why This Matters

The hourly full scan produced bursts of `PurgeInvocation` and `PurgeJournal` commands. The size
of each burst depended on how many invocations expired during the interval, and these bursts
can cause multi-second spikes in write-to-read latency, given how expensive purges usually are.
Smaller, more frequent scans spread the purge work across the interval.

### Impact on Users

- Existing and new deployments get the new behavior on upgrade. No configuration change is needed.
- Completed invocations and journals are still purged within roughly one `cleanup-interval` of
  expiring. The purge work is spread over the interval instead of arriving at once.

### Migration Guidance

No action required.
