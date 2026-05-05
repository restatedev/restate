# Kafka and shuffler batch ingestion are now the default

## Behavioral Change / Breaking Change

### What Changed

The batch ingestion path that was introduced as opt-in experimental in v1.6 is now
the only implementation for both:

- Kafka subscriptions ingestion (`restate-ingress-kafka`).
- Partition-to-partition shuffle (the partition processor's outbox shuffle, which
  forwards messages between partitions).

The two opt-in flags that previously gated this behavior are removed:

- `common.experimental-kafka-batch-ingestion`
- `common.experimental-shuffler-batch-ingestion`

The corresponding legacy code paths (the bifrost-based shuffle state machine and
the legacy Kafka consumer/dispatcher) have been deleted.

### Why This Matters

The batch ingestion path leverages the ingestion client to amortize append work
across multiple records, giving substantially higher throughput on Kafka ingress
and on cross-partition message shuffling.

### Impact on Users

- **Users who never set either flag** (the vast majority): no action required.
  You will pick up the faster batch ingestion path automatically on upgrade.
- **Users who set either flag to `true`**: no behavior change. You can remove
  the keys from your config; they are no longer recognized.
- **Users who explicitly set either flag to `false`** to stay on the legacy
  path: this preference is no longer honored. You will get the batch ingestion
  path. Remove the keys from your config — the legacy implementation has been
  removed and there is no equivalent option to keep using it.

### Migration Guidance

Remove these keys from any `restate.toml` or environment overrides if present:

```toml
[common]
experimental-kafka-batch-ingestion = ...        # remove
experimental-shuffler-batch-ingestion = ...     # remove
```

Equivalent env vars to drop: `RESTATE_COMMON__EXPERIMENTAL_KAFKA_BATCH_INGESTION`,
`RESTATE_COMMON__EXPERIMENTAL_SHUFFLER_BATCH_INGESTION`.

No data migration is required.
