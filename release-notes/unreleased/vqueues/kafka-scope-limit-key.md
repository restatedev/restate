# Release Notes: Kafka ingress scope and limit-key support

## New Feature

### What Changed

Kafka subscription-triggered invocations can now carry scope and limit-key
information via Kafka record headers. This enables vqueue-based concurrency
control for Kafka-ingested invocations.

This feature is gated behind the `kafka-scope` experimental flag (which in
turn requires `vqueues`). When the flag is disabled, the headers are not
inspected.

### How to Use

Enable the flag in your configuration:

```toml
[common.experimental]
enable-vqueues = true
enable-kafka-scope = true
```

Or via environment variables:

```
RESTATE_COMMON_EXPERIMENTAL_ENABLE_VQUEUES=true
RESTATE_COMMON_EXPERIMENTAL_ENABLE_KAFKA_SCOPE=true
```

Then, set the following Kafka record headers on your producer:

- **`x-restate-scope`**: The scope string for the invocation. When present, determines the partition key and acts as a namespace for virtual object instances.
- **`x-restate-limit-key`**: The limit key for hierarchical concurrency/rate limiting. Format: `"level1"` or `"level1/level2"`. Only valid when `restate-scope` is also set.

Example (Java Kafka producer):

```java
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");
record.headers().add("restate-scope", "my-scope".getBytes(StandardCharsets.UTF_8));
record.headers().add("restate-limit-key", "tenant1".getBytes(StandardCharsets.UTF_8));
producer.send(record);
```

### Impact on Users

- **Existing deployments**: No impact. The feature is off by default; Kafka records (with or without these headers) continue to work as before.
- **New usage**: Contributors who want to experiment with vqueue concurrency control for Kafka-ingested invocations can enable the `kafka-scope` experimental flag and set these headers on their Kafka records.
