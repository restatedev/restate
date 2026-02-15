# Plan: Memory Pool Integration in the Invoker

## Context

When a Restate node restarts or gains new partition leaders during failover, all pending
invocations are re-invoked simultaneously, causing unbounded memory growth. The invoker currently
limits concurrency (slot count) but has no mechanism to bound *memory* usage.

This plan describes how to integrate a `MemoryPool` into the invoker to control memory consumption
on **both** paths:

- **Outbound**: reading journal entries from RocksDB and sending them to service deployments
- **Inbound**: reading response messages from deployments and forwarding them through the invoker
  → PP → bifrost pipeline

### Key Upstream References
- Umbrella issue: restatedev/restate#4311
- Memory pool primitives: restatedev/restate#4339 (MemoryPool + MemoryLease + MemoryController)
- Memory tracking in network messages: restatedev/restate#4367
- Networking cleanups (refined lease patterns): restatedev/restate#4393
- Byte-bounded backpressure design: restatedev/restate#4354

### Established Patterns (from PR #4367 / #4393)

1. **Bounded `mpsc::channel(N)` → unbounded channel + MemoryPool permits**: Backpressure is
   enforced by acquiring a `MemoryLease` *before* enqueueing. The lease is carried with the
   message and released on `Drop` when fully consumed.

2. **Named pools via MemoryController**: `TaskCenter::memory_controller().create_pool("name", || capacity_fn())`.

3. **Lease lifecycle**: Held from point of acquisition through to final consumption (e.g., after
   bifrost append, after h2 frame consumption). Operations like `split()`, `merge()`, `take()`
   support batching and transfer patterns (used by log-server writer in PR #4393).

---

## Architecture Overview

### Memory Lease Lifecycles

```
OUTBOUND (RocksDB → deployment):
  ┌─ lease acquired ─────────────────────────────────── lease dropped ─┐
  │                                                                     │
  acquire(entry_size) → read from RocksDB → encode → unbounded_ch → h2 consumes frame
                                                                        ↓
                                                                   MemoryLease::drop()

INBOUND (deployment → bifrost):
  ┌─ lease acquired ────────────────────────────────────────────────────────── lease dropped ─┐
  │                                                                                           │
  decode msg → acquire(msg_size) → send_invoker_tx → invoker loop → Effect → propose → bifrost append
                                                                                              ↓
                                                                                  Arc<Envelope> dropped
                                                                                  → Effect dropped
                                                                                  → MemoryLease::drop()
```

Both paths share a **single** "invoker" memory pool. This provides a unified budget for all
invoker memory. The pool is shared across all concurrent invocations on the node.

### CachedJournal Removal

Currently, the PP sends `InvokeInputJournal::CachedJournal(metadata, items)` containing the
pre-loaded input entry to the invoker (source: `crates/worker/src/partition/state_machine/mod.rs:1298-1394`).
This data originates in PP memory with no invoker pool accounting.

To avoid cross-pool memory transfer complexity, the invoker should **always** read from RocksDB
itself using `InvokeInputJournal::NoCachedJournal`. This is safe because the PP commits the
storage batch before executing the `Action::Invoke` (leadership layer processes actions after
storage commit), so the entry is guaranteed to be in RocksDB by the time the invoker reads it.

**Change**: PP always sends `NoCachedJournal`. The `CachedJournal` variant can be deprecated
(or retained for backward compatibility but never used for new invocations).

---

## Deadlock Analysis

With bidirectional memory accounting using a shared pool, there are more blocking points. The
critical question is whether the system can always make progress when the pool is exhausted.

### Participants and their tasks

| Task | Role | Independent? |
|------|------|-------------|
| InvocationTask (per invocation) | Reads/writes between invoker_rx/http_stream | Async, yields at reserve() |
| h2 connection task | Consumes outbound frames from unbounded channel | Yes (spawned task) |
| Invoker main loop | Forwards InvocationTaskOutput → Effect → output_tx | Single loop, processes one at a time |
| PP leader loop | Receives from output_tx, proposes to bifrost | Independent, processes one at a time |
| Bifrost background appender | Batches and appends to log, drops Arcs | Yes (spawned task) |

### Scenario 1: Pool exhausted, both outbound and inbound waiting

InvocationTask's `bidi_stream_loop` has:
- A pending outbound message waiting for a lease (to send to deployment)
- A pending inbound message waiting for a lease (to forward to PP)

**Resolution**: Both are waiting in `tokio::select!` arms. When memory is freed by either:
- h2 task consuming an outbound frame → outbound lease freed → wakes waiters
- Bifrost appender completing a batch → `Arc<Envelope>` dropped → Effect dropped → inbound
  lease freed → wakes waiters

Both the h2 task and bifrost appender are **independent spawned tasks** that don't depend on the
InvocationTask making progress. So they will eventually free memory, unblocking one of the
select arms. No deadlock.

### Scenario 2: Invoker main loop blocked on output_tx.send().await

The invoker main loop processes `InvocationTaskOutput` messages and forwards them as `Effect` via
`output_tx` (bounded channel, capacity = `internal_queue_length`, default 1000). If the PP leader
is slow, `output_tx.send().await` blocks.

While blocked, the invoker main loop cannot process other invocations' outputs. These pile up in
`invoker_tx` (unbounded). With inbound memory leases inside each `InvocationTaskOutput`, the pool
fills up. New InvocationTasks can't acquire leases.

**Resolution**: The PP leader loop processes effects independently and proposes them to bifrost.
The bifrost background appender is independent. So eventually:
1. Bifrost appender flushes a batch → `Arc<Envelope>` dropped → inbound leases freed
2. PP leader loop drains output_tx → more room for the invoker main loop
3. Invoker main loop resumes → processes queued InvocationTaskOutputs
4. InvocationTasks get memory back → resume

**Caveat**: Throughput may temporarily drop during pressure, but no deadlock occurs because the
bifrost appender always makes independent progress.

### Scenario 3: Cross-invocation starvation

All N concurrent invocations are waiting for memory. Every lease is either:
- In an outbound frame waiting for h2 to consume
- In an Effect waiting for bifrost to append

Both h2 and bifrost appender are independent tasks. They WILL free memory. The only question
is latency: under extreme memory pressure, all invocations slow down together. This is the
desired behavior (graceful degradation vs OOM).

### Scenario 4: Replay loop stall

During `replay_loop`, the runner reads journal entries from RocksDB and writes them to the HTTP
stream. The outbound `write()` acquires a lease. If the pool is exhausted, `write()` blocks.

**Why this is safe**: During replay, the deployment hasn't sent any data yet (it waits to receive
all replay entries). So there are no inbound leases on this invocation. Other invocations may
hold inbound leases, but those are freed by the bifrost appender (independent task). The h2 task
also frees outbound leases from other invocations. Eventually memory becomes available.

**Timeout**: Wrap replay `write()` with a timeout (e.g., 60s). On timeout, fail the invocation
with a transient error for retry. This prevents indefinite stalling.

---

## Implementation Plan

### Step 1: Port MemoryPool primitives to restate-memory crate

Port `MemoryPool`, `MemoryLease`, and `MemoryController` from upstream PR #4339.

**Files to create/modify:**
- `crates/memory/src/pool.rs` (new)
- `crates/memory/src/controller.rs` (new)
- `crates/memory/src/metric_definitions.rs` (new)
- `crates/memory/src/lib.rs` (add module declarations and re-exports)
- `crates/memory/Cargo.toml` (add deps)

### Step 2: Wire MemoryController into TaskCenter

- `crates/core/src/task_center.rs` - Add `MemoryController` to `TaskCenterInner`
- `crates/core/src/task_center/handle.rs` - Add `memory_controller()` accessor
- `crates/node/src/network_server/metrics.rs` - Call `submit_metrics()` during Prometheus render
- `server/src/main.rs` - Call `notify_config_update()` on config changes

### Step 3: Add invoker memory pool configuration

- `crates/types/src/config/worker.rs`:
  ```rust
  /// # Invoker memory limit
  ///
  /// Maximum memory budget for the invoker. This bounds the total memory used for
  /// buffering outbound messages to service deployments AND inbound messages from
  /// deployments awaiting bifrost append. Shared across all concurrent invocations.
  pub invoker_memory_limit: NonZeroByteCount,  // default: 256 MiB
  ```

### Step 4: Remove CachedJournal usage

**Rationale**: The PP currently sends pre-loaded journal items to the invoker via
`InvokeInputJournal::CachedJournal`. This data is in PP memory with no invoker pool accounting.
To avoid cross-pool memory transfer, the invoker should always read from RocksDB itself.

**Changes:**
- `crates/worker/src/partition/state_machine/mod.rs` - In `init_journal()`, always return
  `NoCachedJournal` instead of building `CachedJournal` with the input entry. The invocation
  metadata and journal size are already stored; the invoker will read the entry from RocksDB.
- `crates/invoker-impl/src/invocation_task/mod.rs` - In `select_protocol_version_and_run()`,
  the `CachedJournal` arm can be simplified or deprecated. The invoker always reads from the
  transaction.
- `crates/invoker-api/src/handle.rs` - Optionally deprecate `CachedJournal` variant (or keep
  for backward compatibility but never construct it)

**Safety**: The PP commits the storage batch before executing `Action::Invoke` (the leadership
layer processes actions after storage commit per
`crates/worker/src/partition/leadership/leader_state.rs:515-694`). So journal entries are
guaranteed to be in RocksDB when the invoker reads them.

### Step 5: Add MemoryLease to InvocationTaskOutput and Effect

The inbound lease must travel from the InvocationTask through the entire pipeline to bifrost.

**InvocationTaskOutputInner** (`crates/invoker-impl/src/invocation_task/mod.rs`):
```rust
pub(super) enum InvocationTaskOutputInner {
    NewEntry {
        entry_index: EntryIndex,
        entry: Box<EnrichedRawEntry>,
        requires_ack: bool,
        reservation: MemoryLease,  // NEW: holds memory until bifrost append
    },
    NewCommand {
        command_index: CommandIndex,
        command: journal_v2::raw::RawCommand,
        requires_ack: bool,
        reservation: MemoryLease,  // NEW
    },
    NewNotificationProposal {
        notification: RawNotification,
        reservation: MemoryLease,  // NEW
    },
    // Other variants (Closed, Suspended, Failed, etc.) don't carry data → no lease needed
    // ...
}
```

**Effect** (`crates/invoker-api/src/effects.rs`):
```rust
pub struct Effect {
    pub invocation_id: InvocationId,
    pub kind: EffectKind,
    pub reservation: MemoryLease,  // NEW: carried through to bifrost
}
```

The `restate-invoker-api` crate gains a dependency on `restate-memory`. Non-data-carrying
Effects (Suspended, End, Failed, etc.) use `MemoryLease::unlinked()` or `pool.empty_lease()`.

**Lease transfer chain:**
1. InvocationTask creates `InvocationTaskOutputInner::NewEntry { reservation, .. }`
2. Invoker main loop receives it, transfers `reservation` into `Effect { reservation, .. }`
3. Effect travels through `output_tx` → PP leader → `Command::InvokerEffect(effect)`
4. Command wrapped in `Envelope`, wrapped in `Arc<Envelope>`
5. `Arc<Envelope>` queued in bifrost background appender
6. After `append_batch_erased()` returns, batch is dropped → Arc dropped → Envelope dropped →
   Command dropped → Effect dropped → **MemoryLease dropped** → bytes returned to pool

### Step 6: Implement BytesWithLease for outbound path

Create `BytesWithLease` in `crates/invoker-impl/src/invocation_task/mod.rs`:
```rust
pub(crate) struct BytesWithLease {
    bytes: Bytes,
    _lease: MemoryLease,
}
// impl bytes::Buf delegating to inner Bytes
```

Update type aliases (channel becomes unbounded, pool is the real bound):
```rust
type InvokerBodyStream = StreamBody<ReceiverStream<Result<Frame<BytesWithLease>, Infallible>>>;
type InvokerRequestStreamSender = mpsc::UnboundedSender<Result<Frame<BytesWithLease>, Infallible>>;
```

### Step 7: Thread MemoryPool through InvocationTask

- `DefaultInvocationTaskRunner` stores a `MemoryPool`
- `InvocationTask` gains `pool: MemoryPool` field
- `prepare_request()` creates `mpsc::unbounded_channel()` instead of `mpsc::channel(1)`
- Pool is created via `MemoryController` in `Service::new()` / `Service::run()`

### Step 8: Outbound write() acquires lease BEFORE RocksDB read

In the replay loop, memory should be acquired before loading data from RocksDB:

```rust
// In replay_loop, for each journal entry from the lazy RocksDB stream:
// 1. The lazy iterator yields the entry (minimal memory: entry is in RocksDB block cache)
// 2. Acquire a lease for the encoded message size
// 3. Encode and send with the lease

async fn write(
    &mut self,
    http_stream_tx: &InvokerRequestStreamSender,
    msg: ProtocolMessage,
) -> Result<(), InvokerError> {
    let buf = self.encoder.encode(msg);
    // Acquire lease for the encoded size (blocks if pool exhausted)
    let lease = self.invocation_task.pool.reserve(buf.len()).await;
    let frame = Frame::data(BytesWithLease { bytes: buf, _lease: lease });
    http_stream_tx.send(Ok(frame))
        .map_err(|_| InvokerError::UnexpectedClosedRequestStream)?;
    Ok(())
}
```

**Ideal pre-acquisition**: For even tighter control, we could acquire the lease BEFORE reading
the entry from the RocksDB iterator, using the entry's serialized size from journal metadata (if
available). This prevents the entry from being loaded into memory without a corresponding lease.
However, the lazy journal stream doesn't currently expose per-entry sizes, so the pragmatic first
step is to acquire after encoding (the entry was in RocksDB block cache anyway). Pre-read
acquisition can be added as a follow-up by extending the journal stream to expose entry sizes.

### Step 9: Restructure bidi_stream_loop for bidirectional memory safety

This is the most complex change. The `bidi_stream_loop` must handle memory acquisition for BOTH
outbound and inbound without deadlocking.

**State machine approach**: Maintain two optional "pending" buffers:
- `pending_outbound: Option<Bytes>` - encoded message waiting for outbound lease
- `pending_inbound: Option<(MessageHeader, ProtocolMessage)>` - decoded message waiting for
  inbound lease

```rust
async fn bidi_stream_loop(...) -> TerminalLoopState<()> {
    let mut pending_outbound: Option<Bytes> = None;
    let mut pending_inbound: Option<PendingInbound> = None;

    loop {
        tokio::select! {
            // ── Arm 1: Accept new outbound notification ──
            // Only active when no pending outbound AND no pending inbound
            // (if we had pending inbound, we prioritize draining it)
            opt = self.invocation_task.invoker_rx.recv(),
                if pending_outbound.is_none() && pending_inbound.is_none() =>
            {
                let encoded = self.encode_notification(opt)?;
                match self.invocation_task.pool.try_reserve(encoded.len()) {
                    Some(lease) => self.send_outbound(http_stream_tx, encoded, lease)?,
                    None => { pending_outbound = Some(encoded); }
                }
            },

            // ── Arm 2: Acquire memory for pending outbound ──
            lease = self.invocation_task.pool.reserve(
                pending_outbound.as_ref().map_or(0, |b| b.len())
            ), if pending_outbound.is_some() =>
            {
                let data = pending_outbound.take().unwrap();
                self.send_outbound(http_stream_tx, data, lease)?;
            },

            // ── Arm 3: Read from deployment response stream ──
            // Only active when no pending inbound (backpressure: stop reading
            // from HTTP, which triggers TCP flow control on the deployment)
            chunk = http_stream_rx.next(), if pending_inbound.is_none() => {
                match self.process_response_chunk(chunk)? {
                    ProcessResult::NeedsMemory(msg, size) => {
                        pending_inbound = Some(PendingInbound { msg, size });
                    }
                    ProcessResult::Done => {}
                }
            },

            // ── Arm 4: Acquire memory for pending inbound ──
            lease = self.invocation_task.pool.reserve(
                pending_inbound.as_ref().map_or(0, |p| p.size)
            ), if pending_inbound.is_some() =>
            {
                let pending = pending_inbound.take().unwrap();
                self.forward_inbound_with_lease(pending.msg, lease)?;
            },

            // ── Arm 5: Inactivity timeout ──
            _ = tokio::time::sleep(self.invocation_task.inactivity_timeout) => {
                return TerminalLoopState::Continue(())
            },
        }
    }
}
```

**Key properties:**
- Both `pool.reserve()` arms (2 and 4) can be active simultaneously in the `select!`. Tokio
  polls both. Whichever gets memory first proceeds. No deadlock because memory is freed by
  independent tasks (h2, bifrost appender).
- When we have a pending inbound, we stop reading from `http_stream_rx` (Arm 3 disabled). This
  propagates backpressure via TCP flow control to the deployment.
- When we have a pending outbound, we stop reading from `invoker_rx` (Arm 1 disabled). This
  propagates backpressure to the invoker main loop.
- We avoid reading new outbound when there's a pending inbound (Arm 1 guard:
  `pending_inbound.is_none()`), prioritizing inbound drainage since those leases have a longer
  lifecycle.

### Step 10: Inbound message processing and lease acquisition

In `handle_read()` / `handle_message()` (currently sync), the decoded response message needs a
lease before being forwarded via `send_invoker_tx()`.

**Approach**: `process_response_chunk()` (called from Arm 3) decodes the message, attempts
`try_reserve()` for the decoded message size:
- If successful: forward immediately with the lease
- If pool exhausted: return `ProcessResult::NeedsMemory(msg, size)` which stashes it as
  `pending_inbound`

For the v4+ `DecoderStream`, the approach is similar. The decoded `Message` is the unit of
memory accounting.

**Note**: The decoder may produce multiple messages from one chunk. On pool exhaustion, we process
as many as we can and stash the first one that fails. Remaining undecoded bytes stay in the
decoder buffer. On the next iteration (after Arm 4 acquires the lease), we continue draining
the decoder.

### Step 11: Update response_stream_loop

The `response_stream_loop` (post-bidi phase) only reads from the response stream. Inbound
messages still need leases. The approach is the same: try_reserve, stash as pending if fails,
have a reserve arm in the select.

### Step 12: Release notes and metrics

**Metrics** (via MemoryController):
- `restate.memory_pool.usage_bytes` (gauge, label: name="invoker")
- `restate.memory_pool.capacity_bytes` (gauge, label: name="invoker")

**Release notes** (`release-notes/unreleased/`):
- New `worker.invoker.invoker-memory-limit` configuration (default 256 MiB)
- Behavioral change: invoker now bounds total memory for both outbound and inbound message
  buffering. Under memory pressure, invocations apply backpressure rather than OOM.
- Internal change: invoker no longer receives cached journal items from the partition processor;
  always reads from storage.

---

## Testing Strategy

1. **MemoryLease lifecycle test**: Verify that dropping `BytesWithLease` returns bytes to pool;
   verify that dropping `Effect` returns bytes to pool
2. **Bidirectional select! test**: Small pool (1 KiB). Bidi_stream_loop with both outbound and
   inbound blocked on memory. Verify that when an external task frees memory, the correct arm
   resumes.
3. **End-to-end backpressure test**: Fill the pool, verify TCP flow control kicks in (deployment
   can't send more), verify invocations slow down but don't OOM.
4. **CachedJournal removal test**: Verify invocations succeed when PP always sends
   NoCachedJournal (invoker reads from RocksDB)
5. **Unlimited mode**: `MemoryPool::unlimited()` for backward compatibility; all existing tests
   pass unchanged.

---

## Open Questions

1. **Pool sizing**: Default 256 MiB (matches log-server/worker data service limits from PR #4367).
   Should it be configurable relative to total system memory?

2. **Pre-read acquisition**: Acquiring a lease BEFORE the RocksDB read requires knowing the entry
   size in advance. The journal metadata could be extended to store per-entry sizes, enabling true
   pre-acquisition. Deferred to follow-up.

3. **Per-invocation caps**: Prevent one invocation with a massive journal from starving others.
   Deferred to follow-up (noted in #4354).

4. **output_tx channel interaction**: The bounded `output_tx` channel (capacity 1000) between the
   invoker main loop and the PP leader is a separate bottleneck. With inbound leases, memory is
   held longer when output_tx is full. Consider whether output_tx should also become unbounded
   (relying on the memory pool for bounds instead). This would simplify the pipeline and make
   lease lifetimes more predictable.
