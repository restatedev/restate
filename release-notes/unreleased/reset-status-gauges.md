# Backfill and reset VQueue statistics

## New Feature

When status-gauge collection is first enabled on an existing partition, Restate
reconstructs the service, deployment, and virtual-object counts from retained
VQueue entries before the partition starts. This includes completed entries that
have not yet been deleted.

Initial collection and rebuilding can increase partition startup time and
temporarily require additional local disk space.

### Reset and rebuild

You can reset the stored service, deployment, and virtual-object statistics by
setting the following option and restarting the affected nodes:

```toml
[common]
experimental-enable-status-gauges-reset = true
```

Each local partition clears its statistics when it starts. Collection remains
disabled while this option is enabled, even if status-gauge collection is also
enabled or would normally be automatic. Statistics queries are unavailable
during this time. Invocation and VQueue entry data are preserved.

To rebuild the counts, turn reset mode off, enable collection, and restart again:

```toml
[common]
experimental-enable-status-gauges-reset = false
experimental-enable-track-status-gauges = true
```

The counts are reconstructed from the retained VQueue entries before each local
partition starts. Once collection is enabled, it remains enabled across restarts
even if the collection flag is removed, until explicitly reset again.
