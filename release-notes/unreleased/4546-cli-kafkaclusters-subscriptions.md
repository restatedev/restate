# Release Notes for Issue #4546: CLI commands for Kafka clusters and subscriptions

## New Feature

### What Changed
The `restate` CLI can now manage Kafka clusters and subscriptions directly,
backed by the dynamic Kafka cluster admin API from #4224.

```
restate kafkaclusters | kc        list | create | describe | edit | patch | delete
restate subscriptions | sub       list | create | describe | delete
```

### Examples

```bash
# Kafka clusters
restate kc create my-cluster bootstrap.servers=broker:9092 security.protocol=SASL_SSL
restate kc create my-cluster -f confluent-cloud.properties
restate kc edit my-cluster
restate kc patch my-cluster --set client.id=restate-cli

# Subscriptions
restate sub create kafka://my-cluster/orders service://Counter/count group.id=g1
restate sub list
restate sub describe <id>
```

Run `restate kc --help` or `restate sub --help` for everything else.

### Related Issues
- Issue #4546: CLI Kafka clusters and subscription commands
- Issue #4224: Configure KafkaCluster dynamically
