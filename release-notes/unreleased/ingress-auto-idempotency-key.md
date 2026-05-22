# Release Notes: Auto-inject idempotency key for service and virtual object calls

## Behavioral Change

### What Changed

When the `controlled-idempotent-sharding` cluster feature is enabled, the HTTP
ingress now automatically generates a random ULID-based idempotency key for
calls to services (`InvocationTargetType::Service`) and virtual objects
(`InvocationTargetType::VirtualObject`) that do not already carry an
`Idempotency-Key` header. This applies regardless of whether the call targets
a specific scope. Calls that already provide an idempotency key, and calls to
workflows, are left untouched.

### Why This Matters

The primary motivation is safe, transparent retries at the ingress. With an
idempotency key attached to every request, the ingress can unconditionally
retry an in-flight invocation whenever its connection to the target partition
processor is lost or the partition leader changes, without risking duplicate
execution: the partition processor will deduplicate on the idempotency key
so the caller still observes exactly-once semantics.

This is made practical by the `controlled-idempotent-sharding` feature, which
constrains idempotent invocations to a bounded set of 256 deterministic
partition-key buckets per service (see [[restatectl-provision-features]]).
That controlled scatter width keeps the cost of idempotent bookkeeping
predictable, so it is safe to apply automatically to all otherwise
non-idempotent ingress traffic. Without this bound, blanket key injection
would spread idempotency state across the full partition-key space and
defeat the purpose of the sharding scheme.

Because the injected key is freshly randomized per request, no
cross-request deduplication is introduced — only retries of the same
in-flight request are collapsed.

### Impact on Users

- **Clusters with `controlled-idempotent-sharding` enabled** (the default
  for newly provisioned clusters): service and virtual object calls without
  an `Idempotency-Key` header are now treated as idempotent invocations with
  a server-generated key. They will be routed to one of the 256 controlled
  buckets for the target rather than spread across the full partition-key
  space. Invocation metadata is retained according to the
  idempotent-invocation retention policy.
- **Clusters with `controlled-idempotent-sharding` disabled**: no change.
- **Workflows**: no change.
- **Callers that already send an `Idempotency-Key` header**: no change.

### Migration Guidance

None required. Callers do not need to change anything. Operators who do not
want this behavior can opt out at provisioning time with
`restatectl provision --disable-feature controlled-idempotent-sharding`
(see [[restatectl-provision-features]]); the feature set is frozen in
`NodesConfiguration` and cannot be toggled later.
