# Release Notes: Experimental Ingestion Client

## New Feature (Experimental)

### What Changed

A new experimental ingestion client provides more efficient invocation ingestion, particularly beneficial for Kafka subscriptions. When enabled, invocations are batched and routed through the Partition Processor, which becomes the sole writer to its logs.

### Why This Matters

In certain scenarios, such as consuming from a Kafka topic with few partitions, the new ingestion client can significantly improve ingestion throughput compared to the legacy implementation.

### Impact on Users

- **Experimental**: This feature is disabled by default and should be used with caution
- **Cluster-wide**: All nodes in the cluster must be running v1.6 before enabling this feature
- **No rollback**: Once enabled and data has been ingested, you cannot roll back to a version prior to v1.6

### Usage

To enable the experimental Kafka batch ingestion, set the following environment variable on all nodes:

```bash
RESTATE_EXPERIMENTAL_KAFKA_BATCH_INGESTION=true
```

Or in your configuration file:

```toml
experimental-kafka-batch-ingestion = true
```

**Important**: Ensure all nodes in your cluster are running v1.6 before enabling this feature.

### Related Issues

- [#3976](https://github.com/restatedev/restate/pull/3976): Ingestion client crate
- [#3975](https://github.com/restatedev/restate/pull/3975): Kafka ingress refactor to use ingestion-client
