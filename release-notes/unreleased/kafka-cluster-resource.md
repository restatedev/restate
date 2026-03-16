# Release Notes: Configure Kafka clusters in Admin API

## New Feature

### What Changed
You can now configure `KafkaCluster`s in the Restate Admin API, UI and CLI directly, instead of manually configuring them in the `ingress.kafka-clusters` configuration properties.

For more info on usage, check the Admin API documentation for more details: https://docs.restate.dev/admin-api/kafka_cluster

The usage of the subscription API remains the same.

### Impact on Users
- **Existing Kafka clusters**: Existing Kafka cluster configurations in the `restate-server` configuration files will continue to work.
- **New Kafka clusters**: You can now manage Kafka clusters dynamically via the Admin REST API/UI/CLI.

