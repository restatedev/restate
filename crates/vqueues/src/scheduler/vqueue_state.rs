// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use enum_map::{Enum, EnumMap};
use metrics::counter;
use tokio::time::Instant;

use restate_clock::RoughTimestamp;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, VQueueStore, stats::WaitStats};
use restate_types::vqueues::VQueueId;

use crate::metric_definitions::{
    VQUEUE_CONCURRENCY_RULES_WAIT_MS, VQUEUE_DEPLOYMENT_CONCURRENCY_WAIT_MS,
    VQUEUE_INVOKER_CONCURRENCY_WAIT_MS, VQUEUE_INVOKER_MEMORY_WAIT_MS,
    VQUEUE_INVOKER_THROTTLING_WAIT_MS, VQUEUE_LOCK_WAIT_MS, VQUEUE_THROTTLING_RULES_WAIT_MS,
};
use crate::scheduler::queue::QueueItem;

use super::clock::SchedulerClock;
use super::queue::Queue;
use super::queue_meta::MetaLiteUpdate;
pub use super::queue_meta::VQueueMetaLite;
use super::resource_manager::{AcquireOutcome, PermitBuilder, ResourceKind};
use super::{
    ResourceManager, RunAction, ThrottleScope, UnconfirmedAssignments, VQueueHandle, YieldAction,
};

const QUANTUM: i32 = 1;

pub(super) enum Pop {
    /// The queue needs to receive more credits to be driven.
    NeedsCredit,
    /// An action is ready to be executed.
    Run(RunAction),
    /// Yielding can place back an item from the run queue to inbox, or from inbox to inbox
    /// but with a different run_at time.
    Yield(YieldAction),
    // may get used in the future if per-vqueue throttling is implemented through the
    // same delayed queue of eligibility tracker.
    //
    // When this is generated, also call
    // `head_stats.set_wait(Some(WaitBucket::ThrottlingRules))` (or
    // `InvokerThrottling`, depending on `scope`) so the throttle wait time is
    // attributed correctly. The segment is closed lazily on the next `try_pop`.
    #[allow(dead_code)]
    Throttle {
        delay: Duration,
        /// the reason for throttling
        scope: ThrottleScope,
    },
    /// Queue needs to be moved out of the ready ring. The queue will be woken up
    /// by the resource manager.
    Blocked(ResourceKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::IsVariant)]
pub(super) enum Eligibility {
    Eligible,
    EligibleAt(RoughTimestamp),
    NotEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DetailedEligibility {
    EligibleRunning,
    EligibleInbox,
    Scheduled(RoughTimestamp),
    Empty,
}

impl DetailedEligibility {
    #[inline(always)]
    pub fn as_compact(&self) -> Eligibility {
        match self {
            DetailedEligibility::EligibleRunning | DetailedEligibility::EligibleInbox => {
                Eligibility::Eligible
            }
            DetailedEligibility::Scheduled(ts) => Eligibility::EligibleAt(*ts),
            DetailedEligibility::Empty => Eligibility::NotEligible,
        }
    }
}

/// Every distinct state the head item can be waiting in. A `Stats` instance is
/// in at most one bucket at any moment; transitions are recorded on entry/exit
/// via `Stats::set_wait`, and the elapsed time is the wait time attributed to
/// that bucket.
///
/// `WaitBucket::for_resource` maps every `BlockedOn(ResourceKind)` outcome to a
/// bucket. The match is exhaustive, so adding a new `ResourceKind` is a compile
/// error until a bucket is assigned.
///
/// The throttling buckets (`ThrottlingRules`, `InvokerThrottling`)
/// will be entered when `Pop::Throttle` starts being emitted — that path is not
/// wired up yet — and exited lazily on the next `try_pop` call (which resolves
/// to acquire, block, or re-throttle).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Enum)]
#[repr(u8)]
enum WaitBucket {
    Lock,
    ConcurrencyRules,
    InvokerConcurrency,
    InvokerMemory,
    DeploymentConcurrency,
    ThrottlingRules,
    /// Node-level invoker throttling — entered whenever the item is either
    /// `BlockedOn(ResourceKind::InvokerThrottling)` or delayed on the global
    /// "run" token bucket (same underlying concept, observed from two sides).
    InvokerThrottling,
}

impl WaitBucket {
    fn metric(self) -> &'static str {
        match self {
            WaitBucket::Lock => VQUEUE_LOCK_WAIT_MS,
            WaitBucket::ConcurrencyRules => VQUEUE_CONCURRENCY_RULES_WAIT_MS,
            WaitBucket::InvokerConcurrency => VQUEUE_INVOKER_CONCURRENCY_WAIT_MS,
            WaitBucket::InvokerMemory => VQUEUE_INVOKER_MEMORY_WAIT_MS,
            WaitBucket::DeploymentConcurrency => VQUEUE_DEPLOYMENT_CONCURRENCY_WAIT_MS,
            WaitBucket::ThrottlingRules => VQUEUE_THROTTLING_RULES_WAIT_MS,
            WaitBucket::InvokerThrottling => VQUEUE_INVOKER_THROTTLING_WAIT_MS,
        }
    }

    /// Categorize a `BlockedOn(ResourceKind)` outcome into the wait bucket that
    /// will accumulate the time spent blocked on it.
    ///
    /// The match is exhaustive on purpose: any new `ResourceKind` variant must
    /// choose a bucket here, so accounting can never silently drop on the floor.
    fn for_resource(r: &ResourceKind) -> Self {
        match r {
            ResourceKind::Lock { .. } => WaitBucket::Lock,
            ResourceKind::LimitKeyConcurrency { .. } => WaitBucket::ConcurrencyRules,
            ResourceKind::InvokerConcurrency => WaitBucket::InvokerConcurrency,
            ResourceKind::InvokerMemory => WaitBucket::InvokerMemory,
            ResourceKind::InvokerThrottling => WaitBucket::InvokerThrottling,
            ResourceKind::DeploymentConcurrency => WaitBucket::DeploymentConcurrency,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    /// At most one wait segment is open at any moment — the current blocking reason
    /// (or `None` if not blocked). Making "at most one" a structural invariant is
    /// what prevents the over-attribution bug where two timers run concurrently.
    current: Option<(WaitBucket, Instant)>,
    /// Micros accumulated per bucket. `usize` (≥ 64 bits on supported targets)
    /// is large enough to hold arbitrarily long waits without overflow concerns.
    accumulated_micros: EnumMap<WaitBucket, usize>,
}

impl Stats {
    fn reset(&mut self) {
        // Flush any open segment to the cumulative metric counter before zeroing
        // the accumulators. `reset` is called when the head changes (item removed,
        // preempted, or a higher-priority item replaces it) — the in-memory
        // `WaitStats` for the previous head is by design dropped on the floor,
        // but the node-wide metric counters should still reflect the time we
        // observed the item actually waiting, otherwise the counters systematically
        // under-report whenever items don't make it all the way to `finalize()`.
        self.set_wait(None);
        *self = Self::default();
    }

    /// Transition the wait segment. Passing `None` means "no longer blocked".
    /// Passing the *same* bucket as the currently-open segment is a no-op —
    /// the segment keeps running.
    fn set_wait(&mut self, next: Option<WaitBucket>) {
        if self.current.map(|(b, _)| b) == next {
            return;
        }
        let now = Instant::now();
        if let Some((bucket, since)) = self.current {
            self.accumulate(bucket, now.saturating_duration_since(since));
        }
        self.current = next.map(|b| (b, now));
    }

    fn accumulate(&mut self, bucket: WaitBucket, delay: Duration) {
        counter!(bucket.metric()).increment(delay.as_millis() as u64);
        let micros = usize::try_from(delay.as_micros()).unwrap_or(usize::MAX);
        self.accumulated_micros[bucket] = self.accumulated_micros[bucket].saturating_add(micros);
    }

    fn build_wait_stats(&self, open_segment: Option<(WaitBucket, Duration)>) -> WaitStats {
        let mut micros = self.accumulated_micros;
        if let Some((bucket, delay)) = open_segment {
            let extra = usize::try_from(delay.as_micros()).unwrap_or(usize::MAX);
            micros[bucket] = micros[bucket].saturating_add(extra);
        }
        // Cast usize-micros to u32-ms, saturating so that very long waits clamp to
        // ~49 days per bucket instead of silently wrapping.
        let ms = |bucket| u32::try_from(micros[bucket] / 1000).unwrap_or(u32::MAX);
        WaitStats {
            blocked_on_lock_ms: ms(WaitBucket::Lock),
            blocked_on_concurrency_rules_ms: ms(WaitBucket::ConcurrencyRules),
            blocked_on_invoker_concurrency_ms: ms(WaitBucket::InvokerConcurrency),
            blocked_on_invoker_memory_ms: ms(WaitBucket::InvokerMemory),
            blocked_on_deployment_concurrency_ms: ms(WaitBucket::DeploymentConcurrency),
            blocked_on_throttling_rules_ms: ms(WaitBucket::ThrottlingRules),
            blocked_on_invoker_throttling_ms: ms(WaitBucket::InvokerThrottling),
        }
    }

    fn finalize(&mut self) -> WaitStats {
        self.set_wait(None);
        let stats = self.build_wait_stats(None);
        self.reset();
        stats
    }

    pub fn snapshot(&self) -> WaitStats {
        let open = self.current.map(|(b, since)| (b, since.elapsed()));
        self.build_wait_stats(open)
    }
}

#[derive(derive_more::Debug)]
pub struct VQueueState<S: VQueueStore> {
    pub handle: VQueueHandle,
    pub qid: VQueueId,
    meta: VQueueMetaLite,
    deficit: i32,
    #[debug(skip)]
    unconfirmed_assignments: UnconfirmedAssignments,
    #[debug(skip)]
    queue: Queue<S>,
    head_stats: Stats,
    #[debug(skip)]
    current_permit: PermitBuilder,
}

impl<S: VQueueStore> VQueueState<S> {
    pub fn new(
        qid: VQueueId,
        handle: VQueueHandle,
        meta: VQueueMetaLite,
        num_running: u32,
    ) -> Self {
        let queue = Queue::new(num_running);
        Self {
            handle,
            qid,
            meta,
            deficit: QUANTUM,
            unconfirmed_assignments: UnconfirmedAssignments::new(),
            queue,
            head_stats: Stats::default(),
            current_permit: Default::default(),
        }
    }

    pub fn apply_meta_update(&mut self, update: &MetaLiteUpdate) {
        self.meta.apply_update(update)
    }

    pub fn new_empty(qid: VQueueId, handle: VQueueHandle, meta: VQueueMetaLite) -> Self {
        Self {
            qid,
            handle,
            meta,
            unconfirmed_assignments: UnconfirmedAssignments::new(),
            queue: Queue::new_closed(),
            deficit: 0,
            head_stats: Stats::default(),
            current_permit: Default::default(),
        }
    }

    pub fn try_pop(
        &mut self,
        cx: &mut std::task::Context<'_>,
        storage: &S,
        resources: &mut ResourceManager,
    ) -> Result<Pop, StorageError> {
        let (inbox_head_key, inbox_head_value, is_running) = match self.queue.head() {
            Some(QueueItem::Inbox { key, value }) => (key, value, false),
            Some(QueueItem::Running { key, value }) => (key, value, true),
            e @ Some(QueueItem::None) | e @ None => {
                unreachable!(
                    "cannot pop from empty queue or attempted to pop before polling {:?}/{e:?}",
                    self.handle
                )
            }
        };

        let item_weight = inbox_head_value.weight().get() as i32;
        if self.deficit < item_weight {
            // give credit.
            self.deficit += QUANTUM;
            return Ok(Pop::NeedsCredit);
        } else {
            self.deficit -= item_weight;
        }

        if is_running {
            let action = YieldAction {
                key: *inbox_head_key,
                next_run_at: None,
            };
            self.queue
                .advance(storage, &self.unconfirmed_assignments, &self.qid)?;
            return Ok(Pop::Yield(action));
        }

        match resources.poll_acquire_permit(
            cx,
            self.handle,
            &self.meta,
            inbox_head_key,
            &inbox_head_value.metadata,
            &mut self.current_permit,
        ) {
            AcquireOutcome::Acquired(resources) => {
                self.unconfirmed_assignments
                    .insert(*inbox_head_key, resources);

                let action = RunAction {
                    key: *inbox_head_key,
                    wait_stats: self.head_stats.finalize(),
                };

                self.queue
                    .advance(storage, &self.unconfirmed_assignments, &self.qid)?;
                Ok(Pop::Run(action))
            }
            AcquireOutcome::BlockedOn(resource) => {
                // One call handles the full state transition: closes any previously
                // open segment (for a different resource) and opens the new one.
                self.head_stats
                    .set_wait(Some(WaitBucket::for_resource(&resource)));
                Ok(Pop::Blocked(resource))
            }
        }
    }

    pub fn is_dormant(&self) -> bool {
        // We hold on to the vqueue until we confirm/reject all pending assignments. If we didn't
        // do so, we risk revisiting/redequeuing the unconfirmed items if the vqueue popped back to life
        // (i.e., on enqueue). This is the reason why we check for `unconfirmed_assignments`
        (self.queue.is_empty() || self.meta.is_inbox_empty() || self.meta.is_queue_paused())
            && self.unconfirmed_assignments.is_empty()
    }

    pub fn poll_eligibility(&mut self, storage: &S) -> Result<DetailedEligibility, StorageError> {
        self.queue
            .advance_if_needed(storage, &self.unconfirmed_assignments, &self.qid)?;

        Ok(self.check_eligibility())
    }

    pub fn check_eligibility(&self) -> DetailedEligibility {
        let inbox_head_key = match self.queue.head() {
            Some(QueueItem::Running { .. }) => return DetailedEligibility::EligibleRunning,
            Some(QueueItem::Inbox { key, .. }) => key,
            Some(QueueItem::None) => return DetailedEligibility::Empty,
            None if self.queue.remaining_in_running_stage() > 0 => {
                return DetailedEligibility::EligibleRunning;
            }
            None if self.meta.inbox_len() > 0 => return DetailedEligibility::EligibleInbox,
            None => return DetailedEligibility::Empty,
        };

        // Only applies to inboxed items.
        if inbox_head_key.run_at() > SchedulerClock.now_millis() {
            return DetailedEligibility::Scheduled(inbox_head_key.run_at());
        }

        DetailedEligibility::EligibleInbox
    }

    pub fn notify_removed(&mut self, key: &EntryKey) -> Option<PermitBuilder> {
        if self.queue.remove(key) {
            self.head_stats.reset();
            Some(self.current_permit.take())
        } else {
            None
        }
    }

    pub fn remove_from_unconfirmed_assignments(&mut self, key: &EntryKey) -> Option<PermitBuilder> {
        self.unconfirmed_assignments.remove(key)
    }

    /// Returns true if the head was changed
    pub fn notify_enqueued(&mut self, key: &EntryKey, value: &EntryValue) -> Option<PermitBuilder> {
        if self.queue.enqueue(key, value) {
            self.head_stats.reset();
            Some(self.current_permit.take())
        } else {
            None
        }
    }

    pub fn get_head_wait_stats(&self) -> WaitStats {
        self.head_stats.snapshot()
    }

    /// How many items left in the running stage
    pub fn num_remaining_in_running_stage(&self) -> u32 {
        self.queue.remaining_in_running_stage()
    }

    /// Takes into account the number of unconfirmed assignments when calculating
    /// the inbox length
    pub fn num_waiting_inbox(&self) -> u64 {
        self.meta
            .inbox_len()
            .saturating_sub(self.unconfirmed_assignments.len()) as u64
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Stats, WaitBucket};

    /// Regression test for the "two open wait segments overlap" bug.
    ///
    /// Before the single-current-segment redesign, transitioning from
    /// `BlockedOn(Lock)` to `BlockedOn(LimitKeyConcurrency)` opened the
    /// concurrency-rules timer without closing the lock one, so both timers ran
    /// concurrently and the final `WaitStats` over-attributed time to both
    /// buckets. Here the head waits 5 s on a lock, then 3 s on a concurrency
    /// rule — the sum across buckets must be exactly 8 s, never 13 s.
    #[tokio::test(start_paused = true)]
    async fn transition_between_wait_buckets_does_not_double_count() {
        let mut s = Stats::default();

        s.set_wait(Some(WaitBucket::Lock));
        tokio::time::advance(Duration::from_secs(5)).await;

        s.set_wait(Some(WaitBucket::ConcurrencyRules));
        tokio::time::advance(Duration::from_secs(3)).await;

        let stats = s.finalize();

        assert_eq!(stats.blocked_on_lock_ms, 5_000);
        assert_eq!(stats.blocked_on_concurrency_rules_ms, 3_000);
        // Other buckets never opened — all zero.
        assert_eq!(stats.blocked_on_invoker_concurrency_ms, 0);
        assert_eq!(stats.blocked_on_invoker_memory_ms, 0);
        assert_eq!(stats.blocked_on_deployment_concurrency_ms, 0);
        assert_eq!(stats.blocked_on_throttling_rules_ms, 0);
        assert_eq!(stats.blocked_on_invoker_throttling_ms, 0);

        // finalize() must fully reset: a subsequent finalize is all-zero.
        assert!(s.current.is_none());
        let empty = s.finalize();
        assert_eq!(empty.blocked_on_lock_ms, 0);
        assert_eq!(empty.blocked_on_concurrency_rules_ms, 0);
    }

    /// `set_wait(Some(X))` called twice in a row with the same bucket must keep
    /// the original segment running — not reset the start time — so the full
    /// elapsed wait is attributed on close.
    #[tokio::test(start_paused = true)]
    async fn same_bucket_set_wait_is_idempotent() {
        let mut s = Stats::default();
        s.set_wait(Some(WaitBucket::Lock));
        tokio::time::advance(Duration::from_secs(2)).await;
        s.set_wait(Some(WaitBucket::Lock)); // no-op; segment must continue
        tokio::time::advance(Duration::from_secs(2)).await;

        let stats = s.finalize();
        assert_eq!(stats.blocked_on_lock_ms, 4_000);
    }

    /// `reset()` (called when the head is preempted or the item is removed)
    /// must flush any open segment so the cumulative metric counter still
    /// reflects the wait time, even though the per-item `WaitStats` is
    /// discarded. We verify the flush by observing that a subsequent call to
    /// `set_wait(None)` on the fresh `Stats` doesn't double-count the flushed
    /// time, and that `current` is cleared.
    #[tokio::test(start_paused = true)]
    async fn reset_clears_open_segment() {
        let mut s = Stats::default();
        s.set_wait(Some(WaitBucket::Lock));
        tokio::time::advance(Duration::from_secs(7)).await;
        s.reset();

        // After reset, there's no open segment and no accumulated per-bucket time.
        assert!(s.current.is_none());
        let stats = s.finalize();
        assert_eq!(stats.blocked_on_lock_ms, 0);
    }
}
