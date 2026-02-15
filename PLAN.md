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

2. **Named pools via MemoryController**:
   `TaskCenter::memory_controller().create_pool("name", || capacity_fn())`.

3. **Lease lifecycle**: Held from point of acquisition through to final consumption (e.g., after
   bifrost append, after h2 frame consumption). Operations like `split()`, `merge()`, `take()`
   support batching and transfer patterns (used by log-server writer in PR #4393).

---

## Architecture Overview

### Two Separate Memory Pools

A **single shared pool** for both directions creates a deadlock risk: if all memory is consumed
by outbound frames in the h2 pipeline and the deployment is blocked on sending because we aren't
reading its response (HTTP/2 per-stream flow control), then h2 can't consume outbound frames
(the stream is stalled), and we can't acquire inbound leases to unblock the response read.
Circular dependency → deadlock.

**Solution**: Use two independent pools:

| Pool | Name | Bounds | Released when |
|------|------|--------|---------------|
| **Outbound** | `"invoker-outbound"` | Messages from RocksDB → deployment | h2 consumes frame |
| **Inbound** | `"invoker-inbound"` | Messages from deployment → bifrost | Bifrost append completes |

This eliminates any cross-direction circular dependency. Each direction's memory is released
by an independent spawned task (h2 task for outbound, bifrost background appender for inbound).

### Memory Lease Lifecycles

```
OUTBOUND (RocksDB → deployment):
  peek v.len() ──> pool_out.reserve(size) ──> decode entry ──> encode ──> unbounded_ch ──> h2 frame consumed
                                                                                            ↓
                                                                                       MemoryLease::drop()

INBOUND (deployment → bifrost):
  read header(8B) ──> pool_in.reserve(body_size) ──> read body ──> decode ──> send_invoker_tx
                                                                                    ↓
                                                              invoker loop → Effect → propose → bifrost append
                                                                                                     ↓
                                                                                         Arc<Envelope> dropped
                                                                                         → Effect dropped
                                                                                         → MemoryLease::drop()
```

**Critical property**: Memory leases are acquired BEFORE reading data into invoker memory.
This ensures we never exceed the pool budget.

### CachedJournal Removal

Currently, the PP sends `InvokeInputJournal::CachedJournal(metadata, items)` containing the
pre-loaded input entry to the invoker
(`crates/worker/src/partition/state_machine/mod.rs:1298-1394`). This data originates in PP
memory with no invoker pool accounting.

To avoid cross-pool memory transfer complexity, the invoker should **always** read from RocksDB
itself using `InvokeInputJournal::NoCachedJournal`. This is safe because the PP commits the
storage batch before executing the `Action::Invoke` (leadership layer processes actions after
storage commit), so the entry is guaranteed to be in RocksDB by the time the invoker reads it.

---

## Deadlock Analysis

With separate pools for inbound and outbound, the deadlock analysis simplifies significantly.

### Participants and their tasks

| Task | Role | Independent? |
|------|------|-------------|
| InvocationTask (per invocation) | Reads/writes between invoker_rx/http_stream | Async, yields at reserve() |
| h2 connection task | Consumes outbound frames from unbounded channel | Yes (spawned task) |
| Invoker main loop | Forwards InvocationTaskOutput → Effect → output_tx | Single loop |
| PP leader loop | Receives from output_tx, proposes to bifrost | Independent |
| Bifrost background appender | Batches and appends to log, drops Arcs | Yes (spawned task) |

### Scenario 1: Outbound pool exhausted

All outbound memory is in frames sitting in the unbounded channel or being processed by h2.

**Resolution**: The h2 connection is an independent spawned task. It consumes frames, which drops
`BytesWithLease`, which releases outbound leases. The InvocationTask yields at
`pool_out.reserve().await`, allowing tokio to schedule the h2 task.

Meanwhile, the **inbound path is unaffected** because it uses a separate pool. The select! loop
continues to read from the response stream.

### Scenario 2: Inbound pool exhausted

All inbound memory is in `InvocationTaskOutput` → `Effect` → `Envelope` → bifrost pipeline.

**Resolution**: The bifrost background appender is an independent spawned task. When it completes
`append_batch_erased()`, `Arc<Envelope>` is dropped → Effect dropped → inbound leases freed.

Meanwhile, the **outbound path is unaffected** because it uses a separate pool.

### Scenario 3: Both pools exhausted simultaneously

The InvocationTask has both a pending outbound (waiting for outbound pool) and a pending inbound
(waiting for inbound pool). Both `pool.reserve()` arms are active in the `select!`.

**Resolution**: Both pools are freed by independent tasks:
- Outbound leases freed by h2 task
- Inbound leases freed by bifrost appender task

Neither depends on the InvocationTask making progress. Eventually one or both free memory.
No circular dependency.

### Scenario 4: Deployment blocked on sending (HTTP/2 flow control)

The deployment has data to send but Restate isn't reading. The HTTP/2 receive window fills up.

**Resolution**: Since the inbound pool is separate, the InvocationTask can always acquire inbound
leases (unless the inbound pool itself is full, which is Scenario 2). Reading continues, the
deployment is unblocked, and the outbound stream can progress.

### Scenario 5: Invoker main loop blocked on output_tx

The invoker main loop is blocked on `output_tx.send().await` (bounded channel to PP, capacity
1000). Inbound leases pile up in `invoker_tx`.

**Resolution**: PP leader + bifrost appender make independent progress, eventually draining
output_tx and freeing leases. Throughput drops but no deadlock.

---

## Pre-Read Memory Acquisition

A core design goal is: **never read data into invoker memory without a corresponding lease.**

### Inbound: Two-Phase Decoder

The service protocol uses an 8-byte length-prefixed header:
```
[Message Type (16 bits) | Flags (16 bits) | Frame Length (32 bits)]
```

After reading the 8-byte header, `header.frame_length()` tells us the body size. We split the
existing `Decoder::consume_next()` into two methods:

```rust
impl Decoder {
    /// Try to parse the next message header from buffered data.
    /// After this returns Some(header), the decoder is in WaitingPayload state.
    /// The caller should acquire a memory lease for header.frame_length() bytes
    /// before calling try_consume_body().
    pub fn try_consume_header(&mut self) -> Result<Option<MessageHeader>, EncodingError> { ... }

    /// Consume the message body. Must be in WaitingPayload state.
    /// Returns None if not enough body bytes are buffered yet.
    pub fn try_consume_body(&mut self) -> Result<Option<(MessageHeader, Message)>, EncodingError> { ... }
}
```

The existing `consume_next()` can remain as a convenience that calls both internally.

**Flow in the select! loop**:
1. Read chunk from HTTP → push to decoder
2. `decoder.try_consume_header()` → get `MessageHeader` with `frame_length()`
3. `pool_in.try_reserve(frame_length)` → if succeeds, continue; if fails, stash state
4. `decoder.try_consume_body()` → get decoded message (or wait for more data from HTTP)
5. Forward message with lease

The 8-byte header is read without a lease (negligible, fixed size). The body is only decoded
after the lease is acquired. If the body bytes haven't fully arrived from HTTP yet, the lease
is held while we continue reading from HTTP — but the lease size matches what will be read,
so we don't exceed budget.

### Outbound: Peek-Before-Decode Journal Iterator

The RocksDB journal iterator (`JournalEntryIter` in
`crates/partition-store/src/journal_table_v2/mod.rs:67-110`) uses `iter.item()` which returns
`(&[u8], &[u8])` — a borrowed key-value pair. The value slice's `.len()` gives us the
serialized entry size before any deserialization.

Add a peek method to the journal stream:

```rust
impl JournalEntryIter {
    /// Returns the serialized byte size of the next entry without advancing
    /// or deserializing. Returns None if no more entries.
    pub fn peek_next_size(&self) -> Option<usize> {
        self.iter.item().map(|(_, v)| v.len())
    }
}
```

**Flow in the replay loop**:
1. `journal_stream.peek_next_size()` → get serialized size (sync, zero-copy from RocksDB block cache)
2. `pool_out.reserve(size).await` → acquire lease
3. `journal_stream.next()` → deserialize entry (copies from RocksDB block cache into owned Bytes)
4. Encode → send as `BytesWithLease`

This ensures the entry is only loaded (deserialized/copied) into invoker-managed memory after
the lease is acquired. The peek uses RocksDB's internal buffer (block cache) — zero overhead.

**Note on PinnableSlice**: The iterator path uses `DBRawIteratorWithThreadMode::item()` which
returns borrowed slices from RocksDB's block cache. Point lookups use `get_pinned_cf()` returning
`DBPinnableSlice`. For the iterator-based journal reads, the borrowed slice is sufficient for
peeking. A future optimization could use point lookups with `PinnableSlice` for even more
control, but the current approach is correct and doesn't over-consume.

---

## Cancellation Safety

All `select!` arms must be cancellation-safe: if one arm fires, progress from other arms'
futures must not be lost.

### Analysis of each future

| Future | Cancellation-safe? | Rationale |
|--------|-------------------|-----------|
| `mpsc::Receiver::recv()` | Yes | Per tokio docs; no data consumed until Ready |
| `mpsc::UnboundedReceiver::recv()` | Yes | Same as above |
| `Stream::next()` on DecoderStream | Yes | Data stays in decoder buffer on Pending/cancel |
| `Stream::next()` on HTTP response | Yes | Data stays in hyper buffer on Pending/cancel |
| `MemoryPool::reserve(size)` | Yes | No side effects until Ready (lease returned) |
| `tokio::time::sleep()` | Yes | Per tokio docs |

### Design principle: No multi-step async in a single arm

All multi-step work happens in **synchronous handler blocks** (right side of `=>`), which
always run to completion. State is preserved in local variables between loop iterations.

```rust
// CORRECT: single async future, sync handler
chunk = http_stream.next() => {
    // All of this runs to completion (sync)
    decoder.push(chunk);
    if let Some(header) = decoder.try_consume_header()? {
        match pool_in.try_reserve(header.frame_length()) {
            Some(lease) => { /* try_consume_body, forward */ }
            None => { pending_inbound = Some(PendingHeader { header, size }); }
        }
    }
}

// WRONG: multi-step async that could lose data on cancellation
msg = async {
    let chunk = http_stream.next().await;  // if cancelled after this...
    let lease = pool.reserve(size).await;  // ...chunk is lost
    (chunk, lease)
} => { ... }
```

### Explicit state machine for cross-iteration progress

The loop maintains explicit state variables that survive across iterations:

```rust
// Outbound state
let mut pending_outbound: Option<Bytes> = None;           // encoded, waiting for lease

// Inbound decoder state (separate from the Decoder which holds its own state)
let mut pending_header: Option<(MessageHeader, usize)> = None;  // header parsed, need lease
let mut inbound_lease: Option<MemoryLease> = None;               // lease acquired, need body
let mut pending_forward: Option<(Message, MemoryLease)> = None;  // decoded, need to forward
```

Each of these is set in a sync handler block and checked via `select!` guards on the next
iteration. No data is ever "in flight" inside an async future.

---

## Prerequisites (already in base branch)

This branch is based on `pr4395` which includes the full memory pool PR stack:
- `9352ea635` - MemoryPool, MemoryLease, MemoryController in `crates/memory/`
- `1d848b059` - Memory tracking wired into network messages, MessageRouter, TaskCenter
- `6f252e31b` - LogServer write batch memory reuse
- `c221c621f` - LogServer store timeout bump
- `4302326c1` - LogServer reads moved to background
- `9357e052c` - MetadataServer RocksDB reads on blocking threads

Available APIs:
- `MemoryPool::with_capacity()`, `::unlimited()`, `::try_reserve()`, `::reserve()`, `::empty_lease()`
- `MemoryLease` (RAII, `#[must_use]`): `split()`, `merge()`, `take()`, `grow()`, `shrink()`, `release()`
- `MemoryController::create_pool()`, `::submit_metrics()`, `::notify_config_update()`
- `TaskCenter::memory_controller()` accessor

---

## Implementation Plan

### Step 1: Add invoker memory pool configuration

Add **two** config options to `InvokerOptions` (`crates/types/src/config/worker.rs`):

```rust
/// # Invoker outbound memory limit
///
/// Maximum memory for buffering outbound messages (journal entries, completions)
/// sent from the invoker to service deployments. Shared across all concurrent
/// invocations on this node.
pub invoker_outbound_memory_limit: NonZeroByteCount,  // default: 128 MiB

/// # Invoker inbound memory limit
///
/// Maximum memory for buffering inbound messages (commands, entries) received
/// from service deployments and awaiting bifrost append. Shared across all
/// concurrent invocations on this node.
pub invoker_inbound_memory_limit: NonZeroByteCount,  // default: 128 MiB
```

### Step 2: Remove CachedJournal usage

**Changes:**
- `crates/worker/src/partition/state_machine/mod.rs` - `init_journal()` always returns
  `NoCachedJournal`. The journal size and metadata are already stored; the invoker reads entries
  from RocksDB.
- `crates/invoker-impl/src/invocation_task/mod.rs` - Simplify `select_protocol_version_and_run()`,
  always read from transaction.
- `crates/invoker-api/src/handle.rs` - Deprecate `CachedJournal` variant.

**Safety**: PP commits storage batch before executing `Action::Invoke` per
`crates/worker/src/partition/leadership/leader_state.rs:515-694`.

### Step 3: Add MemoryLease to InvocationTaskOutput and Effect

The inbound lease must travel from the InvocationTask through the entire pipeline to bifrost.

**InvocationTaskOutputInner** (`crates/invoker-impl/src/invocation_task/mod.rs`):
Add a `reservation: MemoryLease` field to `NewEntry`, `NewCommand`, `NewNotificationProposal`.

**Effect** (`crates/invoker-api/src/effects.rs`):
Add a `reservation: MemoryLease` field. Non-data-carrying effects use `pool.empty_lease()`.

**Lease transfer chain:**
1. InvocationTask acquires lease from `pool_in`
2. Lease stored in `InvocationTaskOutputInner::NewEntry { reservation, .. }`
3. Invoker main loop transfers lease into `Effect { reservation, .. }`
4. Effect → `Command::InvokerEffect` → `Envelope` → `Arc<Envelope>`
5. Bifrost background appender calls `append_batch_erased()`
6. Batch dropped → Arc dropped → Effect dropped → **MemoryLease dropped**

### Step 4: Two-phase Decoder

Add `try_consume_header()` and `try_consume_body()` to both decoders:

**`crates/service-protocol/src/message/encoding.rs`** (v1-v3):
**`crates/service-protocol-v4/src/message_codec/encoding.rs`** (v4+):

```rust
impl Decoder {
    /// Parse the next header from buffered data. Returns None if fewer than 8 bytes
    /// available. After returning Some(header), the decoder is in WaitingPayload state.
    pub fn try_consume_header(&mut self) -> Result<Option<MessageHeader>, EncodingError> {
        if !matches!(self.state, DecoderState::WaitingHeader) {
            // Already have a header pending
            if let DecoderState::WaitingPayload(h) = &self.state {
                return Ok(Some(*h));
            }
        }
        if self.buf.remaining() < 8 {
            return Ok(None);
        }
        let header: MessageHeader = self.buf.get_u64().try_into()?;
        // Size limit/warning checks (existing logic)
        self.state = DecoderState::WaitingPayload(header);
        Ok(Some(header))
    }

    /// Consume the message body. Must be in WaitingPayload state.
    /// Returns None if insufficient body bytes buffered.
    pub fn try_consume_body(&mut self) -> Result<Option<(MessageHeader, Message)>, EncodingError> {
        let DecoderState::WaitingPayload(h) = &self.state else {
            return Ok(None);
        };
        if self.buf.remaining() < h.frame_length() as usize {
            return Ok(None);
        }
        // Existing decode logic
        let h = *h;
        let msg = h.message_type()
            .decode(self.buf.take(h.frame_length() as usize))
            .map_err(|e| EncodingError::DecodeMessage(h.message_type(), e))?;
        self.state = DecoderState::WaitingHeader;
        Ok(Some((h, msg)))
    }

    // Existing consume_next() retained as convenience
}
```

### Step 5: Peekable Journal Iterator

Add `peek_next_size()` to `JournalEntryIter`
(`crates/partition-store/src/journal_table_v2/mod.rs`):

```rust
impl JournalEntryIter {
    /// Returns the serialized byte size of the next entry without advancing
    /// the iterator or deserializing. Returns None if no more entries.
    pub fn peek_next_size(&self) -> Option<usize> {
        if self.remaining == 0 {
            return None;
        }
        self.iter.item().map(|(_, v)| v.len())
    }
}
```

This must be exposed through the `InvocationReaderTransaction` trait and the stream adapter.
The stream type becomes a `PeekableJournalStream` that supports both `peek_next_size()` (sync)
and `next()` (yields entries).

### Step 6: Implement BytesWithLease for outbound path

Create `BytesWithLease` in `crates/invoker-impl/src/invocation_task/mod.rs`:
```rust
pub(crate) struct BytesWithLease {
    bytes: Bytes,
    _lease: MemoryLease,
}
// impl bytes::Buf delegating to inner Bytes
```

Update type aliases (channel becomes unbounded):
```rust
type InvokerBodyStream = StreamBody<ReceiverStream<Result<Frame<BytesWithLease>, Infallible>>>;
type InvokerRequestStreamSender = mpsc::UnboundedSender<Result<Frame<BytesWithLease>, Infallible>>;
```

### Step 7: Thread MemoryPools through InvocationTask

- `DefaultInvocationTaskRunner` stores `pool_out: MemoryPool` and `pool_in: MemoryPool`
- `InvocationTask` gains both pool fields
- `prepare_request()` creates `mpsc::unbounded_channel()` instead of `mpsc::channel(1)`
- Pools created via `MemoryController` in `Service::new()` / `Service::run()`

### Step 8: Restructure bidi_stream_loop with explicit state machine

This is the most complex and most critical change. The loop manages explicit state for both
directions, using only sync handler blocks. All async work is in single-future select! arms.

**State variables:**

```rust
// Outbound state (RocksDB/invoker_rx → deployment)
let mut pending_outbound: Option<Bytes> = None;  // encoded, waiting for outbound lease

// Inbound state machine (deployment → bifrost)
enum InboundState {
    /// Ready to read more from HTTP and feed the decoder.
    Idle,
    /// Header parsed, need a lease for body_size bytes before decoding body.
    NeedLease { body_size: usize },
    /// Lease acquired, may need more HTTP data before body is complete.
    HaveLease { lease: MemoryLease },
    /// Full message decoded with lease, ready to forward.
    ReadyToForward { message: DecodedMessage, lease: MemoryLease },
}
let mut inbound_state = InboundState::Idle;
```

**Select loop:**

```rust
loop {
    tokio::select! {
        // ── Arm 1: Accept new outbound notification ──
        // Guard: no pending outbound, inbound is idle (prioritize inbound drainage)
        opt = self.invocation_task.invoker_rx.recv(),
            if pending_outbound.is_none()
            && matches!(inbound_state, InboundState::Idle) =>
        {
            // Handler: encode notification, try_reserve from pool_out
            // If try_reserve fails, stash as pending_outbound
            // All sync — runs to completion
        },

        // ── Arm 2: Acquire outbound lease for pending message ──
        lease = self.invocation_task.pool_out.reserve(
            pending_outbound.as_ref().map_or(0, |b| b.len())
        ), if pending_outbound.is_some() =>
        {
            // Handler: send pending_outbound with lease (sync)
            let data = pending_outbound.take().unwrap();
            self.send_outbound(&http_stream_tx, data, lease)?;
        },

        // ── Arm 3: Read from HTTP response stream ──
        // Guard: only when inbound is Idle OR HaveLease (need body data)
        // NOT when NeedLease (must acquire lease first) or ReadyToForward
        chunk = http_stream_rx.next(),
            if matches!(inbound_state, InboundState::Idle | InboundState::HaveLease { .. }) =>
        {
            // Handler (all sync):
            // 1. Push chunk data to decoder
            // 2. If Idle: try_consume_header(). If got header:
            //    a. try_reserve from pool_in for body_size
            //    b. If ok: try_consume_body(). If ok → forward. Else → HaveLease
            //    c. If no memory: → NeedLease { body_size }
            // 3. If HaveLease: try_consume_body(). If ok → forward with lease
        },

        // ── Arm 4: Acquire inbound lease ──
        lease = self.invocation_task.pool_in.reserve(
            match &inbound_state {
                InboundState::NeedLease { body_size } => *body_size,
                _ => 0,
            }
        ), if matches!(inbound_state, InboundState::NeedLease { .. }) =>
        {
            // Handler (all sync):
            // Got the lease. try_consume_body() — body may or may not be buffered yet.
            // If body available → decode, forward with lease
            // If body not yet available → HaveLease { lease }
        },

        // ── Arm 5: Inactivity timeout ──
        _ = tokio::time::sleep(self.invocation_task.inactivity_timeout) => {
            return TerminalLoopState::Continue(())
        },
    }
}
```

**Key properties:**
- **Cancellation-safe**: Each arm awaits exactly one future. All handler logic is sync.
- **No data loss**: Decoder state, pending_outbound, and inbound_state survive across
  iterations as local variables.
- **Pre-read acquisition**: Inbound leases are acquired after the 8-byte header (which gives
  the body size) but before the body is decoded. Outbound leases are acquired after peeking
  RocksDB value size but before deserializing.
- **No cross-pool deadlock**: Outbound and inbound use separate pools. The `pool_out.reserve()`
  (Arm 2) and `pool_in.reserve()` (Arm 4) are independent and can both be active.
- **Inbound prioritized**: Arm 1 guard includes `inbound_state == Idle`, preventing new outbound
  work while inbound needs attention.

### Step 9: Restructure replay_loop for pre-read acquisition

The replay loop reads journal entries from the peekable journal stream:

```rust
async fn replay_loop(...) -> TerminalLoopState<()> {
    loop {
        tokio::select! {
            // Early error detection from response headers
            got_headers_res = http_stream_rx.next(), if !got_headers => { ... },

            // Read journal entries with pre-read lease acquisition
            () = async {}, if journal_stream.peek_next_size().is_some() => {
                let size = journal_stream.peek_next_size().unwrap();
                // Acquire outbound lease (may block)
                let lease = tokio::time::timeout(
                    REPLAY_MEMORY_TIMEOUT,
                    self.invocation_task.pool_out.reserve(size),
                ).await.map_err(|_| InvokerError::MemoryPoolTimeout)?;

                // Now consume and decode (sync, data goes into leased budget)
                let entry = journal_stream.next().await.unwrap()?;
                self.write_entry_with_lease(http_stream_tx, entry, lease).await?;
            },

            // Stream exhausted
            _ = async {}, if journal_stream.peek_next_size().is_none() => {
                return TerminalLoopState::Continue(());
            },
        }
    }
}
```

**Note**: The replay loop doesn't need the complex inbound state machine because during replay
the deployment hasn't started sending data yet. The response header arm is for early error
detection only.

### Step 10: Update response_stream_loop

The `response_stream_loop` (post-bidi, response-only phase) needs the same inbound state
machine as `bidi_stream_loop`, minus the outbound arms. Use the two-phase decoder with
`pool_in.reserve()` for each message.

### Step 11: Release notes and metrics

**Metrics** (via MemoryController):
- `restate.memory_pool.usage_bytes` (gauge, label: name="invoker-outbound")
- `restate.memory_pool.capacity_bytes` (gauge, label: name="invoker-outbound")
- `restate.memory_pool.usage_bytes` (gauge, label: name="invoker-inbound")
- `restate.memory_pool.capacity_bytes` (gauge, label: name="invoker-inbound")

**Release notes** (`release-notes/unreleased/`):
- New config: `worker.invoker.invoker-outbound-memory-limit` (default 128 MiB)
- New config: `worker.invoker.invoker-inbound-memory-limit` (default 128 MiB)
- Behavioral change: invoker bounds total memory with separate pools for outbound/inbound.
  Under memory pressure, invocations apply backpressure rather than OOM.
- Internal change: invoker no longer receives cached journal items from the PP; always reads
  from storage.

---

## Testing Strategy

1. **Two-phase decoder tests**: Verify `try_consume_header()` returns size, `try_consume_body()`
   returns message. Verify partial data handling (header but not body).
2. **Peekable journal iterator tests**: Verify `peek_next_size()` returns correct size without
   advancing; subsequent `next()` returns the entry.
3. **BytesWithLease lifecycle**: Dropping returns bytes to pool.
4. **Effect lease lifecycle**: Effect → Command → Envelope → Arc → drop releases lease.
5. **Separate pool deadlock prevention**: One pool exhausted, other direction still makes
   progress. Specifically: exhaust outbound pool, verify inbound reads continue.
6. **Cancellation safety**: Cancel the select! loop mid-iteration (e.g., via timeout), verify
   no data loss in decoder buffer or pending state.
7. **End-to-end backpressure**: Fill both pools, verify TCP flow control and invoker_rx
   backpressure engage, invocations slow but don't OOM.
8. **CachedJournal removal**: Existing invocation tests pass with always-NoCachedJournal.
9. **Unlimited mode**: `MemoryPool::unlimited()` for backward compat; all existing tests pass.

---

## Open Questions

1. **Pool sizing**: Default 128 MiB each (256 MiB total). Should these be configurable as
   a single value split internally, or two independent values?

2. **Pre-read for state entries**: The `write_start()` method collects eager state entries
   and sends them in the StartMessage. These could also be large. Should state reading
   also go through the outbound pool? (Lower priority — eager state already has a size limit
   from PR #4351.)

3. **Per-invocation caps**: Prevent one invocation from monopolizing an entire pool. Deferred
   to follow-up (noted in #4354).

4. **output_tx channel interaction**: The bounded `output_tx` channel (capacity 1000) between
   the invoker main loop and PP leader extends inbound lease lifetimes. Consider making it
   unbounded (relying on the inbound pool for bounds) to make lifetimes more predictable.
