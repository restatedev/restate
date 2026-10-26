// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::task::{Context, Wake, Waker};

use enum_map::{Array, Enum, EnumMap};
use futures::FutureExt;
use futures::task::AtomicWaker;
use metrics::gauge;
use tracing::warn;

use restate_bifrost::{CommitToken, WrittenRecords};
use restate_types::logs::Lsn;
use restate_types::sharding::PartitionId;

use crate::metric_definitions::{PARTITION_LABEL, SELF_PROPOSER_INFLIGHT};

// Every flow's credit per round, in records. Flows that propose a record at a time are served in
// round robin, while a batch pays for every record it carries.
const QUANTUM: i64 = 1;

// Every flow gets one bit in `Shared::woken`.
const _: () = assert!(
    <<SelfProposerSchedulerFlow as Enum>::Array<()> as Array>::LENGTH <= u16::BITS as usize
);

/// The state that's shared with the per-flow wakers. Wakers can be invoked from any thread,
/// so they only record the wake notification here. The notifications are then drained by the
/// scheduler on its next poll.
struct Shared {
    /// Bit `i` is set if flow `i` was woken up since the last time it was drained.
    woken: AtomicU16,
    parent_waker: AtomicWaker,
}

impl Shared {
    /// Sets the wake notification of the flow, returning whether it was already set.
    fn set_woken(&self, flow: SelfProposerSchedulerFlow) -> bool {
        let previous = self.woken.fetch_or(flow.bit(), Ordering::AcqRel);
        previous & flow.bit() != 0
    }

    /// Clears the wake notification of the flow, returning whether it was set.
    fn take_woken(&self, flow: SelfProposerSchedulerFlow) -> bool {
        let previous = self.woken.fetch_and(!flow.bit(), Ordering::AcqRel);
        previous & flow.bit() != 0
    }
}

/// The per-flow waker that's passed to the flow's poll method. When woken up,
/// it marks the flow as woken and wakes up the scheduler, which will put the
/// flow back in the ready queue.
struct FlowWaker {
    flow: SelfProposerSchedulerFlow,
    shared: Arc<Shared>,
}

impl Wake for FlowWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let already_woken = self.shared.set_woken(self.flow);
        // Only wake the parent on the first notification, otherwise a wake up is already pending.
        if !already_woken {
            self.shared.parent_waker.wake();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum State {
    /// The flow reported Poll::Pending, the last time it was polled.
    Pending,
    /// The flow is currently in the ready queue.
    Queued,
    /// This flow is currently being polled, and we haven't heard back a feedback
    /// from the scheduler decision. Wake notifications that arrive while the flow is
    /// being polled are captured in [`Shared::woken`] and are checked when the flow
    /// reports back `Poll::Pending`.
    Polling,
}

#[derive(Debug, Clone)]
struct FlowState {
    /// Signed DRR deficit counter in records (weighted by the flow's cost multiplier).
    ///
    /// Positive values are available service credits. Negative values are allowed
    /// and it means that the flow is currently in debit (because proposal sizes are
    /// charged after the polling decision). A flow with a negative deficit
    /// won't get polled again until its deficit goes positive again over the rounds.
    deficit: i64,
    state: State,
}

#[derive(Debug, Clone, Enum, Copy, PartialEq)]
pub(crate) enum SelfProposerSchedulerFlow {
    Invoker,
    Timer,
    Shuffle,
    Cleaner,
    UpsertSchema,
    UpsertRuleBook,
    NetworkService,
    PartitionMaintenance,
    Scheduler,
}

impl SelfProposerSchedulerFlow {
    /// Returns the cost multiplier for a flow. The multiplier is multiplied by the number of records proposed for that
    /// flow, resulting into it being charged more for those records. A higher multiplier biases the scheduler "against"
    /// that flow (i.e. deprioritizing it).
    fn cost_multiplier(&self) -> u32 {
        match self {
            // The cleaner's records are expensive to process.
            SelfProposerSchedulerFlow::Cleaner => 10,
            SelfProposerSchedulerFlow::Invoker => 1,
            SelfProposerSchedulerFlow::Timer => 1,
            SelfProposerSchedulerFlow::Shuffle => 1,
            SelfProposerSchedulerFlow::UpsertSchema => 1,
            SelfProposerSchedulerFlow::UpsertRuleBook => 1,
            SelfProposerSchedulerFlow::NetworkService => 1,
            SelfProposerSchedulerFlow::PartitionMaintenance => 1,
            SelfProposerSchedulerFlow::Scheduler => 1,
        }
    }

    /// The flow's bit in [`Shared::woken`].
    fn bit(self) -> u16 {
        1 << self.into_usize()
    }

    fn as_str(self) -> &'static str {
        match self {
            SelfProposerSchedulerFlow::Invoker => "invoker",
            SelfProposerSchedulerFlow::Timer => "timer",
            SelfProposerSchedulerFlow::Shuffle => "shuffle",
            SelfProposerSchedulerFlow::Cleaner => "cleaner",
            SelfProposerSchedulerFlow::UpsertSchema => "upsert-schema",
            SelfProposerSchedulerFlow::UpsertRuleBook => "upsert-rule-book",
            SelfProposerSchedulerFlow::NetworkService => "network-service",
            SelfProposerSchedulerFlow::PartitionMaintenance => "partition-maintenance",
            SelfProposerSchedulerFlow::Scheduler => "scheduler",
        }
    }
}

/// The scheduler's decision of which flow to poll next. The caller must then poll the returned flow and
/// report back the results via methods on this struct. Failing to report back the results will result in a panic.
pub(crate) struct SchedulerDecision<'a> {
    /// The flow that's in turn to be polled according to the scheduler's decision.
    pub(crate) flow: SelfProposerSchedulerFlow,
    /// The flow specific waker to be passed to the flow's poll method.
    /// Once it's woken up, this flow moves to the ready queue.
    pub(crate) waker: &'a Waker,
    state: &'a mut EnumMap<SelfProposerSchedulerFlow, FlowState>,
    ready_ring: &'a mut VecDeque<SelfProposerSchedulerFlow>,
    inflight: &'a mut InflightTracker,
    shared: &'a Shared,
    received_feedback: bool,
}

impl<'a> SchedulerDecision<'a> {
    fn flow_state(&mut self) -> &mut FlowState {
        &mut self.state[self.flow]
    }

    fn requeue(&mut self, force_back: bool) {
        let flow = self.flow;
        let state = &mut self.state[flow];
        if state.deficit > 0 && !force_back {
            self.ready_ring.push_front(flow);
        } else {
            self.ready_ring.push_back(flow);
        }
        state.state = State::Queued;
    }

    /// Report back the result of handling the flow to the self-proposer. A flow that managed to
    /// write something will be automatically considered ready to be polled again. Depending on its
    /// deficit, it may get re-enqueued at the head or the back of the ready queue.
    ///
    /// Note: Flows that report back 0 records written will lose their position in the
    /// ready queue to avoid starvation.
    pub(crate) fn on_proposal_enqueued(mut self, result: WrittenRecords) {
        self.received_feedback = true;
        let cost = result
            .records_written
            .saturating_mul(self.flow.cost_multiplier() as usize) as i64;
        let state = self.flow_state();
        state.deficit = state.deficit.saturating_sub(cost);
        std::debug_assert_matches!(state.state, State::Polling);
        self.requeue(result.records_written == 0);
        self.inflight.add(self.flow, result);
    }

    /// Reports back that the flow was ready, but reported an error without proposing any records.
    /// Will be treated as an empty proposal (see [`SelfProposerScheduler::on_proposal_enqueued`]).
    pub(crate) fn on_error(self) {
        self.on_proposal_enqueued(WrittenRecords::default())
    }

    /// Reports back to the scheduler that the flow was polled and reported `Poll::Pending`.
    /// The flow won't get polled again unless it calls `Waker::wake` on the waker that was passed
    /// to its poll method.
    pub(crate) fn on_pending(mut self) {
        self.received_feedback = true;
        assert!(
            matches!(self.flow_state().state, State::Polling),
            "state is not polling, this is a bug"
        );
        if self.shared.take_woken(self.flow) {
            // The flow was woken up while being polled. To avoid starvation, we're going
            // to move the flow back to the back of the ready queue.
            self.requeue(/* force back */ true);
        } else if self.flow_state().deficit >= 0 {
            // We're truly pending, reset the deficit and move on.
            let state = self.flow_state();
            state.state = State::Pending;
            state.deficit = 0;
        } else {
            // The stream is in debit, it should stay in the ready queue until the debit is paid.
            self.requeue(/* force back */ true);
        }
    }
}

impl Drop for SchedulerDecision<'_> {
    fn drop(&mut self) {
        if self.received_feedback {
            // We're good nothing to do here.
            return;
        }
        // This is a bug. We should have received a feedback from whoever executes the decision.
        // Crash in debug builds, and just re-enqueue with a warning in release builds (while stealing its deficit).
        debug_assert!(self.received_feedback);
        warn!(
            "Scheduler decision for flow {:?} dropped without receiving feedback",
            self.flow
        );
        // Steal its deficit if it's positive so that it can get added at the back of the ready ring.
        let state = self.flow_state();
        state.deficit = state.deficit.min(0);
        self.requeue(/* force back */ true);
    }
}

struct FlowMetrics {
    in_flight: metrics::Gauge,
}

impl Drop for FlowMetrics {
    fn drop(&mut self) {
        self.in_flight.set(0.0);
    }
}

struct InflightBatch {
    // Using MaybeDone to fuse the commit token to its resolved LSN once completed.
    commit: futures::future::MaybeDone<CommitToken>,
    // Records written per flow.
    inflight: EnumMap<SelfProposerSchedulerFlow, usize>,
}

struct InflightTracker {
    // Batch Tracking
    batches: VecDeque<InflightBatch>,
    current_batch: EnumMap<SelfProposerSchedulerFlow, usize>,

    // Stat Aggregates
    total_per_flow_inflight: EnumMap<SelfProposerSchedulerFlow, usize>,
    total_inflight: usize,

    // Metrics
    metrics: EnumMap<SelfProposerSchedulerFlow, FlowMetrics>,
}

impl InflightTracker {
    fn new(partition_id: PartitionId) -> Self {
        Self {
            batches: VecDeque::new(),
            current_batch: Default::default(),

            total_per_flow_inflight: Default::default(),
            total_inflight: 0,

            metrics: EnumMap::from_fn(|flow: SelfProposerSchedulerFlow| FlowMetrics {
                in_flight: gauge!(SELF_PROPOSER_INFLIGHT, PARTITION_LABEL => partition_id.to_string(), "flow" => flow.as_str()),
            }),
        }
    }

    fn add(&mut self, flow: SelfProposerSchedulerFlow, written: WrittenRecords) {
        let records = written.records_written;
        self.current_batch[flow] += records;
        self.total_per_flow_inflight[flow] += records;
        self.total_inflight += records;
    }

    fn drop_batch(&mut self, batch: InflightBatch) {
        for (flow, records) in batch.inflight.iter() {
            self.total_per_flow_inflight[flow] =
                self.total_per_flow_inflight[flow].saturating_sub(*records);
            self.total_inflight = self.total_inflight.saturating_sub(*records);
        }
    }

    fn finalize_batch(&mut self, commit_token: CommitToken) {
        self.batches.push_back(InflightBatch {
            commit: futures::future::maybe_done(commit_token),
            inflight: std::mem::take(&mut self.current_batch),
        });
    }

    // Returns whether any batch was dropped.
    fn poll_on_progress(&mut self, cx: &mut Context<'_>, applied_lsn: Lsn) -> bool {
        let mut progressed = false;
        while let Some(batch) = self.batches.front_mut() {
            // `MaybeDone` fuses the commit token: once it resolves it keeps reporting `Ready` and
            // re-exposes its output, so it's safe to poll it again on subsequent calls. It's
            // `Unpin`, so we can pin the field in place without moving it out.
            if batch.commit.poll_unpin(cx).is_pending() {
                break;
            }

            // Resolving the output borrows `batch.commit`; copy the committed LSN out so that the
            // borrow ends before we possibly drop the batch below.
            let committed = match Pin::new(&mut batch.commit).output_mut() {
                Some(Ok(committed)) => Ok(committed.lsn),
                Some(Err(e)) => {
                    debug!(?e, "Failed to commit inflight batch, dropping it");
                    Err(())
                }
                None => unreachable!("commit token resolved but has no output"),
            };

            match committed {
                // The head batch is committed but not applied yet. Batches are applied in order,
                // so the ones behind it can't be applied either.
                Ok(committed_lsn) if applied_lsn < committed_lsn => break,
                // This batch is already applied (or failed), either way, drop it.
                Ok(_) | Err(()) => {
                    let batch = self.batches.pop_front().unwrap();
                    self.drop_batch(batch);
                    progressed = true;
                }
            }
        }
        progressed
    }

    fn report_metrics(&self) {
        for (flow, metrics) in self.metrics.iter() {
            metrics
                .in_flight
                .set(self.total_per_flow_inflight[flow] as f64);
        }
    }
}

/// A scheduler that sits in front of the self-proposer to decide which one of the possible self-proposer writers
/// should be allowed to write next. This is a DRR scheduler that tries to ensure fairness among the writers based on
/// the number of records each one has proposed, weighted by the flow's cost multiplier.
///
/// Because the scheduler doesn't know the cost of a flow beforehand, flows report back their sizes after getting polled.
/// This means that we allow flows to go into negative deficits. A flow with a negative deficit won't get polled again
/// until its deficit goes positive again over the rounds. Compared to typical DRR schedulers, flows with negative deficits
/// will get their deficit refilled even if they're no longer ready.
pub(crate) struct SelfProposerScheduler {
    shared: Arc<Shared>,
    state: EnumMap<SelfProposerSchedulerFlow, FlowState>,
    ready_ring: VecDeque<SelfProposerSchedulerFlow>,
    /// A cache for the per-flow wakers
    wakers: EnumMap<SelfProposerSchedulerFlow, Waker>,

    // Inflight proposal tracking
    inflight: InflightTracker,
}

impl SelfProposerScheduler {
    pub(crate) fn new(partition_id: PartitionId) -> SelfProposerScheduler {
        let state = EnumMap::from_fn(|_| FlowState {
            deficit: 0,
            state: State::Queued,
        });
        let shared = Arc::new(Shared {
            woken: AtomicU16::new(0),
            // The parent waker will be registered on the first poll and keep getting updated there.
            parent_waker: AtomicWaker::new(),
        });
        let wakers = EnumMap::from_fn(|flow| {
            Waker::from(Arc::new(FlowWaker {
                flow,
                shared: Arc::clone(&shared),
            }))
        });
        Self {
            shared,
            // All flows must be ready once on creation so that we can poll them immediately
            // and register their wakers.
            ready_ring: VecDeque::from_iter(state.iter().map(|(flow, _)| flow)),
            state,
            wakers,
            inflight: InflightTracker::new(partition_id),
        }
    }

    /// Moves the flows that got woken up since the last poll to the ready queue.
    fn drain_woken(&mut self) {
        let woken = self.shared.woken.swap(0, Ordering::AcqRel);
        if woken == 0 {
            return;
        }
        for (flow, state) in self.state.iter_mut() {
            if woken & flow.bit() == 0 {
                continue;
            }
            match state.state {
                State::Pending => {
                    state.state = State::Queued;
                    self.ready_ring.push_back(flow);
                }
                // Already in the ready queue, nothing to do.
                State::Queued => {}
                State::Polling => {
                    unreachable!("no flow can be polling while the scheduler is being polled")
                }
            }
        }
    }

    /// Called whenever a batch of proposals have been enqueued for appending.
    pub fn on_round_end(&mut self, token: CommitToken) {
        self.inflight.finalize_batch(token);
        self.inflight.report_metrics();
    }

    /// Drops the inflight batches that are committed at or below `applied_lsn`. Should be called
    /// from the partition processor's loop as the applied LSN advances.
    pub fn poll_on_progress(&mut self, cx: &mut Context<'_>, applied_lsn: Lsn) {
        let progressed = self.inflight.poll_on_progress(cx, applied_lsn);
        if progressed {
            self.inflight.report_metrics();
        }
    }

    /// Polls the scheduler for the next decision. The returned [`SchedulerDecision`] is a handle to the flow that's
    /// scheduled for polling. The caller then should poll the flow inside the decision while passing the associated waker.
    /// The caller MUST report back the result of the poll (via the methods of [`SchedulerDecision`]), dropping the decision
    /// without doing so will result in a panic.
    /// This function returns None if there are currently no flows that are ready to be polled.
    #[must_use]
    pub fn poll_next_ready(&mut self, cx: &mut Context<'_>) -> Option<SchedulerDecision<'_>> {
        // Register the waker before draining the notifications to not miss any wake ups in between.
        self.shared.parent_waker.register(cx.waker());
        self.drain_woken();

        loop {
            let round_length = self.ready_ring.len();
            if round_length == 0 {
                return None;
            }

            let mut closest_deficit = i64::MIN;
            for _ in 0..round_length {
                let flow = self
                    .ready_ring
                    .pop_front()
                    .expect("guarded by the round_length check earlier");
                let state = &mut self.state[flow];
                std::debug_assert_matches!(state.state, State::Queued);

                if state.deficit <= 0 {
                    state.deficit += QUANTUM;
                }

                if state.deficit > 0 {
                    state.state = State::Polling;
                    return Some(SchedulerDecision {
                        flow,
                        waker: &self.wakers[flow],
                        state: &mut self.state,
                        ready_ring: &mut self.ready_ring,
                        inflight: &mut self.inflight,
                        shared: &self.shared,
                        received_feedback: false,
                    });
                } else {
                    self.ready_ring.push_back(flow);
                    closest_deficit = closest_deficit.max(state.deficit);
                }
            }

            // We did one full round and no flows were returned. It means that all flows are in debit.
            // As an optimization, let's fast forward the rounds until we find an eligible flow.
            // To do that we:
            //   - Find the flow with the deficit closest to zero.
            //   - Calculate how many rounds we need to take to positive.
            //   - Fast forward every flow by the estimated number of rounds.
            assert!(closest_deficit <= 0);
            let rounds_to_skip = closest_deficit.saturating_abs() / QUANTUM;
            self.ready_ring.iter().for_each(|flow| {
                self.state[*flow].deficit += rounds_to_skip * QUANTUM;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Wake, Waker};

    use restate_bifrost::{CommitToken, Committed, WrittenRecords};
    use restate_types::logs::Lsn;
    use restate_types::sharding::PartitionId;

    use crate::partition::leadership::self_proposer_scheduler::{
        QUANTUM, SelfProposerSchedulerFlow,
    };

    fn committed(lsn: u64) -> Committed {
        Committed {
            lsn: Lsn::new(lsn),
            at: tokio::time::Instant::now(),
        }
    }

    /// A proposal of `records_written` records. The scheduler only accounts for the records, so
    /// the byte count is not meaningful in these tests.
    fn proposal(records_written: usize) -> WrittenRecords {
        WrittenRecords {
            bytes_written: records_written * 100,
            records_written,
        }
    }

    struct TestWaker {
        woken: AtomicBool,
    }

    impl TestWaker {
        fn clear(&self) {
            self.woken.store(false, Ordering::Relaxed);
        }
    }

    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn works() {
        let test_waker = Arc::new(TestWaker {
            woken: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&test_waker));
        let mut cx = Context::from_waker(&waker);

        let mut scheduler = super::SelfProposerScheduler::new(PartitionId::new_unchecked(1));

        // First poll, expecting the invoker flow.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);

        // Exercise the path where we call the waker inline.
        dec.waker.wake_by_ref();
        // Parent waker should have been woken.
        assert!(test_waker.woken.load(Ordering::Relaxed));
        test_waker.clear();
        // Reporting pending now, should still re-enqueue the flow given the inline wake,
        // but at the back of the ready queue to avoid starvation.
        dec.on_pending();

        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        let timer_waker = dec.waker.clone();
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_pending();

        // Consume the rest of the flows
        for expected_flow in [
            SelfProposerSchedulerFlow::Shuffle,
            SelfProposerSchedulerFlow::Cleaner,
            SelfProposerSchedulerFlow::UpsertSchema,
            SelfProposerSchedulerFlow::UpsertRuleBook,
            SelfProposerSchedulerFlow::NetworkService,
            SelfProposerSchedulerFlow::PartitionMaintenance,
            SelfProposerSchedulerFlow::Scheduler,
        ] {
            let dec = scheduler
                .poll_next_ready(&mut cx)
                .expect("expected flow, got none");
            assert_eq!(dec.flow, expected_flow);
            dec.on_pending();
        }

        // The invoker got re-enqueued after the inline wake.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        // A record consumes the invoker's credit for the round...
        dec.on_proposal_enqueued(proposal(1));

        // ...but it's the only queued flow, so it gets the next round's credit right away.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        dec.on_pending();

        // Nothing more is ready now
        assert!(scheduler.poll_next_ready(&mut cx).is_none());

        // The timer waker is invoked
        timer_waker.wake();
        assert!(test_waker.woken.load(Ordering::Relaxed));
        test_waker.clear();
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_pending();
    }

    #[test]
    fn fast_forwarding() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut scheduler = super::SelfProposerScheduler::new(PartitionId::new_unchecked(1));

        // Report 100x the quantum for the invoker
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        dec.on_proposal_enqueued(proposal(100 * QUANTUM as usize));

        // Report 50x the quantum for the timer
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_proposal_enqueued(proposal(50 * QUANTUM as usize));

        // Report pending for all other flows
        // Consume the rest of the flows
        for expected_flow in [
            SelfProposerSchedulerFlow::Shuffle,
            SelfProposerSchedulerFlow::Cleaner,
            SelfProposerSchedulerFlow::UpsertSchema,
            SelfProposerSchedulerFlow::UpsertRuleBook,
            SelfProposerSchedulerFlow::NetworkService,
            SelfProposerSchedulerFlow::PartitionMaintenance,
            SelfProposerSchedulerFlow::Scheduler,
        ] {
            let dec = scheduler
                .poll_next_ready(&mut cx)
                .expect("expected flow, got none");
            assert_eq!(dec.flow, expected_flow);
            dec.on_pending();
        }

        // At this point, we have the invoker with 100x the quantum debit, and the timer with 50x the quantum debit.
        // Polling the scheduler now will fast forward the rounds until the timer is eligible.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_pending();

        // And now we fast forward to the invoker.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        dec.on_pending();

        // Now all are pending
        assert!(scheduler.poll_next_ready(&mut cx).is_none());
    }

    #[test]
    fn shares_records() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut scheduler = super::SelfProposerScheduler::new(PartitionId::new_unchecked(1));

        // The invoker proposes batches of 4 records and the timer one record at a time, both
        // always ready; the other flows are pending. The invoker gets a quarter of the turns, and
        // the same number of records.
        let (mut invoker, mut timer) = (0, 0);
        while invoker + timer < 1000 {
            let dec = scheduler.poll_next_ready(&mut cx).expect("a flow is ready");
            match dec.flow {
                SelfProposerSchedulerFlow::Invoker => {
                    invoker += 1;
                    dec.on_proposal_enqueued(proposal(4));
                }
                SelfProposerSchedulerFlow::Timer => {
                    timer += 1;
                    dec.on_proposal_enqueued(proposal(1));
                }
                _ => dec.on_pending(),
            }
        }
        assert_eq!((invoker, timer), (200, 800));
    }

    #[test]
    fn tracks_inflight_records() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut scheduler = super::SelfProposerScheduler::new(PartitionId::new_unchecked(1));

        // The invoker proposes 1 KiB across 2 records, which uses up its credit for the round.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        dec.on_proposal_enqueued(WrittenRecords {
            bytes_written: 1024,
            records_written: 2,
        });

        // The timer proposes 512 B across a single record.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_proposal_enqueued(WrittenRecords {
            bytes_written: 512,
            records_written: 1,
        });

        // The inflight total accounts for everything proposed by both flows.
        assert_eq!(scheduler.inflight.total_inflight, 2 + 1);

        // Ending the round seals the current batch, but it stays inflight until applied.
        let (tx, rx) = tokio::sync::oneshot::channel::<Committed>();
        scheduler.on_round_end(CommitToken::new(rx));
        assert_eq!(scheduler.inflight.batches.len(), 1);

        // The commit token hasn't resolved yet, so nothing can be dropped.
        scheduler.inflight.poll_on_progress(&mut cx, Lsn::new(5));
        assert_eq!(scheduler.inflight.batches.len(), 1);

        // The batch commits at LSN 10, but the applied LSN (5) hasn't reached it yet: it's still
        // inflight, and the resolved token isn't polled again.
        tx.send(committed(10)).expect("receiver is alive");
        scheduler.inflight.poll_on_progress(&mut cx, Lsn::new(5));
        assert_eq!(scheduler.inflight.batches.len(), 1);
        assert_eq!(scheduler.inflight.total_inflight, 2 + 1);

        // Records proposed in the next (not yet sealed) round must not be affected by dropping
        // the previous batch.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        let next_flow = dec.flow;
        dec.on_proposal_enqueued(WrittenRecords {
            bytes_written: 256,
            records_written: 1,
        });

        // Once the applied LSN reaches the batch's commit LSN, it's dropped and stops counting.
        scheduler.inflight.poll_on_progress(&mut cx, Lsn::new(10));
        assert!(scheduler.inflight.batches.is_empty());
        assert_eq!(scheduler.inflight.total_inflight, 1);
        assert_eq!(scheduler.inflight.current_batch[next_flow], 1);

        // Sealing and applying the second round drains everything.
        let (tx, rx) = tokio::sync::oneshot::channel::<Committed>();
        scheduler.on_round_end(CommitToken::new(rx));
        tx.send(committed(11)).expect("receiver is alive");
        scheduler.inflight.poll_on_progress(&mut cx, Lsn::new(11));
        assert!(scheduler.inflight.batches.is_empty());
        assert_eq!(scheduler.inflight.total_inflight, 0);
    }
}
