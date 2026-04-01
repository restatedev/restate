# Release Notes: Configure Kafka clusters in Admin API

## New Feature

### What Changed
You can now configure `KafkaCluster`s in the Restate Admin API, UI and CLI directly, instead of manually configuring them in the `ingress.kafka-clusters` configuration properties.

For more info on usage, check the Admin API documentation for more details: https://docs.restate.dev/admin-api/kafka_cluster

The usage of the subscription API remains the same.

### Impact on Users
- **Existing Kafka clusters**: Existing Kafka cluster configurations in the `restate-server` configuration files will continue to work.
- **New Kafka clusters**: You can now manage Kafka clusters dynamically via the Admin REST API/UI/CLI.
- **Name conflicts**: If a cluster with the same name is already configured in the static configuration, the Admin API will reject the request with an error. Conversely, if you add a static configuration entry with the same name as a cluster already configured through the UI/CLI/Admin API, the latter takes precedence and the static configuration entry is ignored.

