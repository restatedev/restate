# Plan: Memory Pool Integration in the Invoker

## Context

When a Restate node restarts or gains new partition leaders during failover, all pending
invocations are re-invoked simultaneously, causing unbounded memory growth. The invoker currently
limits concurrency (slot count) but has no mechanism to bound *memory* usage.

This plan describes how to integrate a `MemoryPool` (from upstream PR #4339) into the invoker to
control memory consumption when reading journal entries from RocksDB and sending them to service
deployments.

### Key Upstream References
- Umbrella issue: restatedev/restate#4311
- Memory pool primitives: restatedev/restate#4339 (MemoryPool + MemoryLease + MemoryController)
- Memory tracking in network messages: restatedev/restate#4367
- Networking cleanups (refined lease patterns): restatedev/restate#4393
- Byte-bounded backpressure design: restatedev/restate#4354

### Established Patterns (from PR #4367 / #4393)

The upstream PR stack establishes a consistent pattern for memory-bounded message passing:

1. **Bounded `mpsc::channel(N)` → unbounded channel + MemoryPool permits**: The channel itself
   becomes unbounded; backpressure is enforced by acquiring a `MemoryLease` from a shared pool
   *before* enqueueing. The lease is carried with the message through the pipeline and released
   on `Drop` when the message is fully consumed.

2. **Named pools via MemoryController**: Each subsystem creates a named pool via
   `TaskCenter::memory_controller().create_pool("name", || config_capacity_fn())`. The controller
   handles deduplication, metrics, and live capacity updates.

3. **BackPressureMode**: Services choose between `PushBack` (async wait for memory) and `Lossy`
   (immediate reject when exhausted). The invoker should use `PushBack` since dropping messages
   is not acceptable.

4. **No `MemoryLease::unlinked()` dance**: PR #4393 refined the pattern so that messages are only
   constructed *after* the memory lease is obtained, avoiding the initial `unlinked()` +
   `swap_reservation()` two-step.

5. **Lease lifecycle**: The lease is held from the point of message encoding through to final
   consumption (e.g., h2 frame consumption, RocksDB commit). Operations like `split()`, `merge()`,
   `take()` support batching and transfer patterns.

---

## Architecture Overview

### Current Message Flow (outbound to deployment)

```
                    encode(msg)
RocksDB/invoker_rx ────────────> mpsc::channel(1) ──> StreamBody ──> hyper/h2 ──> Deployment
                                      ^
                             backpressure: 1 slot
                             (message-count, not bytes)
```

### Target Message Flow (with MemoryPool)

```
                    encode(msg)    pool.reserve(len)
RocksDB/invoker_rx ────────────> ─────────────────> unbounded_channel ──> StreamBody ──> hyper/h2 ──> Deployment
                                       ^                                       |
                                  blocks if pool                               |
                                  exhausted                                    |
                                       |                                       |
                                       <───── MemoryLease dropped when ────────┘
                                              h2 consumes the frame
```

### Response path (inbound from deployment)

The response path (deployment → partition processor) does **not** go through the memory pool.
It is naturally bounded by TCP flow control and the h2 receive window. The `invoker_tx` channel
is unbounded but the partition processor drains it promptly. Adding memory accounting to the
response path would introduce bidirectional deadlock risk (see Deadlock Analysis below).

---

## Deadlock Analysis

This is the most critical aspect of the design. The `InvocationTask` and `ServiceProtocolRunner`
run as a single async task with multiple concurrent loops that interleave reads and writes. A
shared memory pool introduces new blocking points that can cause deadlocks if not carefully handled.

### Scenario 1: Bidi Stream Loop Send-Receive Deadlock

In `bidi_stream_loop` (v1-v3: `service_protocol_runner.rs:401-447`, v4+:
`service_protocol_runner_v4.rs:440-491`), the loop uses `tokio::select!` to alternate between:
- Reading `invoker_rx` → writing to `http_stream_tx` (sending completions/entries to deployment)
- Reading `http_stream_rx` (receiving commands/entries from deployment)
- Inactivity timeout

**Problem**: If `write()` now needs to `pool.reserve(len).await` before sending on the channel,
and the pool is exhausted, the entire `select!` loop blocks. While blocked, the task cannot read
from `http_stream_rx`. If the deployment is itself blocked waiting for Restate to consume its
response data (e.g., it has hit its own send buffer limits), neither side makes progress → deadlock.

**Mitigation**: Restructure `bidi_stream_loop` so that memory acquisition for the outbound path
never prevents the inbound stream from being polled. The approach is a "pending outbound" state
machine:

1. Read a notification/entry from `invoker_rx` and encode it immediately
2. Attempt `try_reserve()` (non-blocking). If it succeeds, send immediately.
3. If `try_reserve()` fails, stash the encoded bytes as "pending outbound" and disable the
   `invoker_rx` arm of the `select!` (since we can only buffer one pending message)
4. Add a new `pool.reserve()` arm to the `select!` that fires when memory becomes available
5. The `http_stream_rx` arm remains always active — inbound is never blocked

This ensures the response stream is always drained even when the outbound path is waiting for
memory. **Key invariant: never hold a `.await` on memory reservation while not polling the
inbound stream.**

### Scenario 2: Replay Loop Stall

During `replay_loop`, the runner reads journal entries from RocksDB (via lazy stream) and writes
them to the HTTP stream while also polling for response headers.

**Problem**: If `write()` blocks on memory acquisition during replay, the response header polling
arm of the `select!` cannot proceed.

**Mitigation**: During replay, the response side only expects headers (not data messages). The
deployment buffers the request body independently of sending response headers. In practice, this
cannot deadlock because the deployment cannot produce response data until it has received all
replay entries. We can safely use `pool.reserve().await` here. However, to avoid indefinite stalls
when the pool is globally exhausted, we should wrap the reservation in a timeout and fail the
invocation with a transient error for retry.

### Scenario 3: Global Pool Exhaustion (Cross-Invocation)

All N concurrent invocations are in `reserve().await` waiting for memory from the shared pool. No
invocation can release memory because they're all blocked on sending. Meanwhile, h2 frames are in
flight and permits will be released when h2 consumes them.

**Why this is safe**: The `reserve()` is async and yields to the tokio runtime. The h2 connection
is driven by a separate spawned task (see `ResponseStream::initialize` at
`invocation_task/mod.rs:481-491` which spawns `client.call(req)`). When an invoker task yields at
`reserve().await`, tokio can poll the h2 connection task, which consumes frames from the unbounded
channel, which drops `BytesWithLease`, which releases the `MemoryLease`, which returns bytes to
the pool and wakes waiting tasks via `Notify`.

**Requirement**: The h2 connection must always be driven by a separate spawned task. This is
already the case in the current code and must remain so.

### Scenario 4: Large Message Starvation

An invocation needs to send a large message (e.g., 30 MiB) but the pool only has small fragments
available because many other invocations hold small leases.

**Mitigation**: The pool's `reserve()` uses `Notify`, which wakes all waiters when memory is
released. As small leases are released, eventually enough contiguous capacity will be available
for the large reservation. This may take longer under high contention but will not deadlock.
For future work: per-invocation caps, priority queues, or acquire timeouts (as noted in #4354).

---

## Implementation Plan

### Step 1: Port MemoryPool primitives to restate-memory crate

Port `MemoryPool`, `MemoryLease`, and `MemoryController` from upstream PR #4339. This is the
foundation for all subsequent steps.

**API surface** (from PR #4339):
```rust
// MemoryPool
pub const fn unlimited() -> Self;                         // zero-overhead, no tracking
pub fn with_capacity(capacity: NonZeroByteCount) -> Self; // bounded pool
pub fn try_reserve(&self, size: usize) -> Option<MemoryLease>;    // non-blocking
pub async fn reserve(&self, size: usize) -> MemoryLease;          // async, waits via Notify
pub fn empty_lease(&self) -> MemoryLease;                          // zero-size lease
pub fn set_capacity(&self, capacity: impl Into<NonZeroByteCount>); // live update

// MemoryLease (RAII, #[must_use])
pub fn size(&self) -> ByteCount;
pub fn release(&mut self) -> usize;      // explicitly return memory
pub fn split(&mut self, amount: usize) -> MemoryLease;
pub fn merge(&mut self, other: MemoryLease);
pub fn take(&mut self) -> MemoryLease;
// On Drop: returns bytes to pool, wakes waiters

// MemoryController
pub fn create_pool(name: &'static str, updater: impl Fn() -> NonZeroByteCount) -> MemoryPool;
pub fn submit_metrics(&self);            // report usage/capacity gauges
pub fn notify_config_update(&self);      // re-read capacities from updater closures
```

**Files to create/modify:**
- `crates/memory/src/pool.rs` (new)
- `crates/memory/src/controller.rs` (new)
- `crates/memory/src/metric_definitions.rs` (new)
- `crates/memory/src/lib.rs` (add module declarations and re-exports)
- `crates/memory/Cargo.toml` (add deps: `hashbrown`, `metrics`, `parking_lot`, `tokio`, `tracing`,
  `restate-serde-util`)

### Step 2: Wire MemoryController into TaskCenter

Following the pattern from PR #4367:

- `crates/core/src/task_center.rs` - Add `MemoryController` field to `TaskCenterInner`, initialize
  with `MemoryController::default()`
- `crates/core/src/task_center/handle.rs` - Add `memory_controller() -> &MemoryController` accessor
- `crates/node/src/network_server/metrics.rs` - Call `memory_controller.submit_metrics()` during
  Prometheus render
- `server/src/main.rs` - Call `memory_controller.notify_config_update()` when config watcher fires

### Step 3: Add invoker memory pool configuration

Add a new config option for the invoker memory budget, following the pattern of
`log_server.data_service_memory_limit` and `worker.data_service_memory_limit` from PR #4367.

**Files to modify:**
- `crates/types/src/config/worker.rs`:
  ```rust
  /// # Invoker memory limit
  ///
  /// Maximum memory budget for buffering outbound messages to service deployments.
  /// This pool is shared across all concurrent invocations on this node.
  /// When exhausted, invocations will apply backpressure (wait for memory to be freed)
  /// rather than consuming unbounded memory.
  pub invoker_memory_limit: NonZeroByteCount,  // default: 256 MiB
  ```

### Step 4: Implement BytesWithLease wrapper

Following the design from issue #4354 and the patterns from PR #4367/4393.

Create `BytesWithLease` in `crates/invoker-impl/src/invocation_task/mod.rs`:

```rust
/// Bytes wrapper that holds a MemoryLease. The lease is released when the
/// h2 layer consumes this frame (i.e., when this value is dropped).
/// This is the mechanism that releases memory back to the pool.
pub(crate) struct BytesWithLease {
    bytes: Bytes,
    _lease: MemoryLease,
}
```

`BytesWithLease` must implement `bytes::Buf` (delegating to inner `Bytes`) so it can be used
as the body data type for `http_body::Frame<BytesWithLease>`.

Update the type aliases:
```rust
// Before:
type InvokerBodyStream = StreamBody<ReceiverStream<Result<Frame<Bytes>, Infallible>>>;
type InvokerRequestStreamSender = mpsc::Sender<Result<Frame<Bytes>, Infallible>>;

// After:
type InvokerBodyStream = StreamBody<ReceiverStream<Result<Frame<BytesWithLease>, Infallible>>>;
type InvokerRequestStreamSender = mpsc::UnboundedSender<Result<Frame<BytesWithLease>, Infallible>>;
```

The channel becomes **unbounded** because the MemoryPool is the actual bound.

### Step 5: Thread MemoryPool through InvocationTask

Plumb the pool reference through the component hierarchy:

1. **`DefaultInvocationTaskRunner`** stores a `MemoryPool` (created via MemoryController in
   `Service::new()` / `Service::run()`)
2. **`InvocationTaskRunner` trait** updated: `start_invocation_task()` signature gains pool
3. **`InvocationTask::new()`** takes a `MemoryPool` parameter, stores it as a field
4. **`prepare_request()`** changes:
   - `mpsc::channel(1)` → `mpsc::unbounded_channel()`
   - Returns `InvokerRequestStreamSender` (now unbounded) + request
   - Takes `&MemoryPool` as parameter (or accessed via `InvocationTask` field)

**Files to modify:**
- `crates/invoker-impl/src/lib.rs` - `DefaultInvocationTaskRunner` gains `pool: MemoryPool`,
  `InvocationTaskRunner::start_invocation_task()` passes it, `Service::new()` creates pool
- `crates/invoker-impl/src/invocation_task/mod.rs` - `InvocationTask` gains `pool: MemoryPool`
  field, `prepare_request()` creates unbounded channel

### Step 6: Update write() methods to acquire leases

The `write()` and `write_raw()` methods in both protocol runners encode the message and then
acquire a memory lease before sending through the unbounded channel.

**In `service_protocol_runner.rs` and `service_protocol_runner_v4.rs`:**

```rust
async fn write(
    &mut self,
    http_stream_tx: &InvokerRequestStreamSender,  // now unbounded
    msg: ProtocolMessage,  // or Message for v4
) -> Result<(), InvokerError> {
    trace!(restate.protocol.message = ?msg, "Sending message");
    let buf = self.encoder.encode(msg);
    let len = buf.len();

    // Acquire memory from the pool (blocks if exhausted)
    let lease = self.invocation_task.pool.reserve(len).await;

    let frame = Frame::data(BytesWithLease { bytes: buf, _lease: lease });
    http_stream_tx.send(Ok(frame))
        .map_err(|_| InvokerError::UnexpectedClosedRequestStream)?;
    Ok(())
}
```

Note: `send()` on `mpsc::UnboundedSender` is synchronous (infallible unless receiver dropped),
so there's no second await point after the lease is acquired.

### Step 7: Restructure bidi_stream_loop for deadlock safety

This is the critical change. The `bidi_stream_loop` must never block on memory acquisition while
not polling the inbound response stream.

**Approach for both v1-v3 (`service_protocol_runner.rs`) and v4+ (`service_protocol_runner_v4.rs`):**

```rust
async fn bidi_stream_loop(...) -> TerminalLoopState<()> {
    // Pending outbound: encoded bytes waiting for a memory lease
    let mut pending_outbound: Option<Bytes> = None;

    loop {
        tokio::select! {
            // Arm 1: Read a new notification/completion from the invoker main loop
            // Only active when we don't already have a pending outbound message
            opt_notification = self.invocation_task.invoker_rx.recv(),
                if pending_outbound.is_none() =>
            {
                match opt_notification {
                    Some(notification) => {
                        let encoded = self.encode_notification(notification);
                        // Try non-blocking reserve first
                        match self.invocation_task.pool.try_reserve(encoded.len()) {
                            Some(lease) => {
                                // Fast path: memory available, send immediately
                                let frame = Frame::data(BytesWithLease {
                                    bytes: encoded, _lease: lease
                                });
                                if http_stream_tx.send(Ok(frame)).is_err() {
                                    return TerminalLoopState::Failed(
                                        InvokerError::UnexpectedClosedRequestStream
                                    );
                                }
                            }
                            None => {
                                // Slow path: stash as pending, will wait for memory
                                pending_outbound = Some(encoded);
                            }
                        }
                    }
                    None => return TerminalLoopState::Continue(()),
                }
            },

            // Arm 2: Wait for memory to become available for the pending outbound message
            lease = self.invocation_task.pool.reserve(
                pending_outbound.as_ref().map_or(0, |b| b.len())
            ), if pending_outbound.is_some() =>
            {
                let data = pending_outbound.take().unwrap();
                let frame = Frame::data(BytesWithLease { bytes: data, _lease: lease });
                if http_stream_tx.send(Ok(frame)).is_err() {
                    return TerminalLoopState::Failed(
                        InvokerError::UnexpectedClosedRequestStream
                    );
                }
            },

            // Arm 3: Read from the deployment response stream (ALWAYS active)
            chunk = http_stream_rx.next() => {
                // ... existing response handling (unchanged) ...
            },

            // Arm 4: Inactivity timeout
            _ = tokio::time::sleep(self.invocation_task.inactivity_timeout) => {
                debug!("Inactivity detected, going to suspend invocation");
                return TerminalLoopState::Continue(())
            },
        }
    }
}
```

**Key properties:**
- The inbound response stream (Arm 3) is **always** polled, regardless of memory pressure
- At most one outbound message is buffered (the pending one), limiting memory overhead
- When memory is available, the fast path (`try_reserve`) avoids the async overhead
- The `invoker_rx` is only polled when there's no pending outbound (backpressure propagates
  to the invoker main loop, which will stop sending notifications/completions)

### Step 8: replay_loop and write_start memory acquisition

During replay, use the straightforward `pool.reserve().await` in `write()`. This is safe because:
- The response side only expects headers during replay (no bidirectional data deadlock)
- The deployment cannot produce response data until replay completes
- Journal entries are read lazily from RocksDB (PR #4346 already merged), so memory is acquired
  per-entry rather than materializing the entire journal

For safety against global pool exhaustion stalls, wrap the replay `write()` calls with a generous
timeout (e.g., 60s), failing the invocation with a transient error for retry.

### Step 9: Add metrics and release notes

**Metrics** (automatic via MemoryController):
- `restate.memory_pool.usage_bytes` (gauge, label: name="invoker")
- `restate.memory_pool.capacity_bytes` (gauge, label: name="invoker")

**Release notes** (`release-notes/unreleased/`):
- New `worker.invoker.invoker-memory-limit` configuration option (default 256 MiB)
- Behavioral change: invoker now bounds memory usage for outbound messages via a shared memory pool
  and applies backpressure when the budget is exhausted

---

## Testing Strategy

1. **Unit tests for BytesWithLease**: Verify `Buf` implementation delegates correctly, verify
   that dropping `BytesWithLease` returns bytes to the pool
2. **Memory pool integration test**: Exhaust the pool, verify that `reserve()` blocks until a
   lease is dropped, verify `try_reserve()` returns `None`
3. **Deadlock safety test**: With a small pool (e.g., 1 KiB), run a bidi_stream_loop scenario
   where the outbound path is blocked on memory while the inbound path continues to receive
   messages. Verify no deadlock occurs.
4. **Unlimited mode test**: Verify `MemoryPool::unlimited()` mode works with zero overhead
   (no blocking, no tracking). This is the fallback when no memory limit is configured.
5. **Existing invoker tests**: All existing tests in `crates/invoker-impl/src/lib.rs::tests`
   should pass unchanged (they should use `MemoryPool::unlimited()` implicitly or explicitly).

---

## Open Questions

1. **Pool sizing default**: PR #4367 uses 256 MiB for `log_server.data_service_memory_limit`
   and `worker.data_service_memory_limit`. Should the invoker match this? Or should it be
   smaller/larger given that the invoker may have many concurrent invocations?

2. **Per-invocation caps**: Should there be a per-invocation maximum memory reservation? This
   prevents one invocation with a large journal from monopolizing the entire pool. Noted as
   future work in issue #4354.

3. **Interaction with concurrency quota**: When memory is bounded, the concurrency limit becomes
   a secondary control. For now, keep both mechanisms independent. Future: auto-tune concurrency
   based on memory availability (issue #2402).

4. **Response path memory accounting**: The current plan does NOT account for memory on the
   response path (deployment → partition processor). This is deliberate to avoid bidirectional
   deadlock. If response memory becomes a problem, it would need a separate pool or a
   fundamentally different approach (e.g., TCP receive window tuning).
