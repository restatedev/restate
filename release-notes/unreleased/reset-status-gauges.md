# Reset and rebuild VQueue statistics

## New Feature

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
partition starts. This scan can increase startup time on large stores.
