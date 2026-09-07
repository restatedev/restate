# Release Notes for Issue #2007: Idempotent Kafka subscription creation

## Bug Fix

### What Changed

Creating a Kafka subscription with the same source topic and sink handler as an existing
subscription now returns the existing subscription instead of inserting a second one.

### Why This Matters

Each subscription starts its own Kafka consumer group. Registering the same source and sink twice
used to create two independent consumers, so the same records were delivered (and invoked) twice.

### Impact on Users

- Repeated `POST /subscriptions` calls (or `restate subscriptions create`) with the same source
  and sink are now idempotent and keep a single consumer.
- Distinct source or sink URIs still create distinct subscriptions.
- Options on a retry are ignored when a matching subscription already exists.

### Migration Guidance

No action is required. Existing duplicate subscriptions are not removed automatically; delete the
unwanted ones if you already created extras.

### Related Issues

- Issue #2007: Kafka subscriptions can be made multiple times for the same source and sink
