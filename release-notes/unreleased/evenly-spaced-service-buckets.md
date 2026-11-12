# Release Notes: Even spread of service invocations across partitions

## Behavioral Change / New Feature

### What Changed

Invocations of services (without a key) can now be spread evenly across partitions. Previously,
some partitions could receive up to about twice the average share of a single service's
invocations while others received far less.

- From v1.8.2, service calls without an idempotency key are spread evenly on all clusters.
- From v1.8.2, newly provisioned clusters can opt in to spreading idempotent calls and Kafka
  subscriptions evenly as well.
- From v1.9.0, this will be the default for newly provisioned clusters.

### Impact on Users

- Existing clusters: after upgrading, service calls without an idempotency key are spread evenly.
  Calls with an idempotency key (including the keys Restate generates automatically for HTTP
  ingress calls) and Kafka subscriptions keep their current placement, because changing it would
  break deduplication. This also applies after upgrading to v1.9.0.
- New clusters: to spread all service invocations evenly, enable the option on the node that
  provisions the cluster, before provisioning it:

  ```toml
  experimental-enable-evenly-spaced-service-buckets = true
  ```

  The option is only read when the cluster is provisioned. Setting or removing it on an already
  provisioned cluster has no effect.

### Migration Guidance

No action is needed. A cluster provisioned with
`experimental-enable-evenly-spaced-service-buckets` enabled cannot be rolled back to a version
older than v1.8.2.
