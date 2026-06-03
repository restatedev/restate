# Release Notes: Auto-inject idempotency key for service and virtual object calls

## Behavioral Change

### What Changed

When the `controlled-idempotent-sharding` cluster feature is enabled, the HTTP
ingress now automatically generates a random idempotency key for
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
caps the number of distinct vqueues an idempotent invocation can land on for
a given service. It does so by routing the invocation to one of a bounded,
deterministic set of 255 partition-key buckets per service (see
[[restatectl-provision-features]]); because a vqueue is keyed by partition
key together with service identity, capping the buckets caps the vqueues.
Without that bound, blanket key injection would spawn a fresh vqueue per
request and overwhelm the scheduler's per-vqueue accounting, which assumes a
manageable number of vqueues per service.

Because the injected key is freshly randomized per request, no
cross-request deduplication is introduced — only retries of the same
in-flight request are collapsed.

### Impact on Users

- **Clusters with `controlled-idempotent-sharding` enabled** (the default
  for newly provisioned clusters): service and virtual object calls without
  an `Idempotency-Key` header are now treated as idempotent invocations with
  a server-generated key. They will be routed to one of the 255 controlled
  buckets for the target rather than spread across the full partition-key
  space. Invocation metadata is retained according to the
  idempotent-invocation retention policy. As a side effect, the invocation's
  value (its result) is now retained after completion for the duration of the
  idempotency retention period.
- **Clusters with `controlled-idempotent-sharding` disabled**: no change.
- **Workflows**: no change.
- **Callers that already send an `Idempotency-Key` header**: no change.

### Migration Guidance

None required. Callers do not need to change anything. Operators who do not
want this behavior can opt out at provisioning time with
`restatectl provision --disable-feature controlled-idempotent-sharding`
(see [[restatectl-provision-features]]). For now, the feature set is frozen
in `NodesConfiguration` at provisioning time and cannot be toggled later.

#### Matching the previous cleanup behavior

The retention/cleanup window for these calls changes because the injected key
makes them count as idempotent invocations. Previously, service and virtual
object calls without an idempotency key were cleaned up according to the
**journal retention** alone. With a key now present, the completion (the
invocation's value) is retained for the **idempotency retention**, and the
journal retention is capped so it never exceeds the idempotency retention.

So cleanup is now governed by *both* retention values rather than journal
retention alone. Each value is taken from the per service/handler setting if
one is configured (via the SDK / Admin API), otherwise from the server
default:

- **Idempotency retention** — now drives how long the invocation metadata and
  value are retained for these calls. Server default:
  `default-idempotency-retention`, with an upper bound of
  `max-idempotency-retention`.
- **Journal retention** — still applies, but is capped at the idempotency
  retention. Server default: `default-journal-retention`, with an upper bound
  of `max-journal-retention`.

To get the exact previous cleanup window for these calls, set the
idempotency retention to the journal retention you relied on before — either
per service/handler (preferred) via the SDK / Admin API, or globally by
setting `default-idempotency-retention` to match `default-journal-retention`.
