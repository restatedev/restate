# Release Notes: Even spread of service invocations across partitions

## Behavioral Change

### What Changed

Invocations of services (without a key) are now spread evenly across partitions. Previously, some
partitions could receive up to about twice the average share of a single service's invocations
while others received far less.

### Impact on Users

- Newly provisioned clusters: all invocations of a service, including idempotent ones, are
  spread evenly across all partitions. This is enabled automatically through the new
  `evenly-spaced-service-buckets` cluster feature, which is listed among the enabled features
  when provisioning.
- Existing clusters: after upgrading, service calls without an idempotency key are spread evenly
  as well. Calls with an idempotency key (including the keys Restate generates automatically for
  HTTP ingress calls) keep their current placement, because changing it would break
  deduplication. Kafka subscriptions also keep their current placement.

### Migration Guidance

No action is needed. Note that a cluster provisioned with this version cannot be downgraded to an
earlier version: older versions do not recognize the `evenly-spaced-service-buckets` feature and
cannot read the cluster's configuration. Existing clusters that are upgraded to this version can
still be rolled back as usual.
