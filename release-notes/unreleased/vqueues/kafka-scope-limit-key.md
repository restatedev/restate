# Release Notes: Kafka ingress scope and limit-key support

## New Feature

### What Changed

Kafka subscription-triggered invocations can now carry scope and limit-key
information via Kafka record headers. This enables vqueue-based concurrency
control for Kafka-ingested invocations.

### How to Use

Set the following Kafka record headers on your producer:

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

- **Existing deployments**: No impact. Kafka records without these headers continue to work as before (no scope, no limit key).
- **New usage**: Users who want to leverage vqueue concurrency control for Kafka-ingested invocations can now set these headers on their Kafka records.
