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
use std::sync::Arc;
use std::task::Poll;
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use enum_map::{Enum, EnumMap};
use futures::FutureExt;
use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use tokio::time::Instant;
use tracing::warn;

use restate_bifrost::CommitToken;
use restate_platform::sync::Mutex;
use restate_types::config::WorkerOptions;
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;

use crate::metric_definitions::{
    PARTITION_LABEL, SELF_PROPOSER_INFLIGHT, SELF_PROPOSER_PENDING,
    SELF_PROPOSER_RECEIVE_TO_PROPOSE, SELF_PROPOSER_WINDOW_BLOCKED_MS,
};

use super::Error;

const QUANTUM: i64 = 64 * 1024; // 64KiB

/// The per-flow waker that's passed to the flow's poll method. When woken up,
/// it puts the flow back in the ready queue.
struct FlowWaker {
    flow: SelfProposerSchedulerFlow,
    inner: Arc<Mutex<Inner>>,
}

impl Wake for FlowWaker {
    fn wake(self: Arc<Self>) {
        let mut guard = self.inner.lock();
        let Inner {
            state, ready_ring, ..
        } = &mut *guard;
        let state = &mut state[self.flow];

        let waker = match state.state {
            State::Pending => {
                state.state = State::Queued;
                ready_ring.push_back(self.flow);
                guard.parent_waker.clone()
            }
            State::Queued => {
                // nothing to do
                None
            }
            State::Polling { notified } => {
                if notified {
                    // we're already notified, nothing to do
                    return;
                }
                state.state = State::Polling { notified: true };
                guard.parent_waker.clone()
            }
        };
        drop(guard);
        if let Some(w) = waker {
            w.wake_by_ref()
        }
    }
}

#[derive(Debug, Clone)]
enum State {
    /// The flow reported Poll::Pending, the last time it was polled.
    Pending,
    /// The flow is currently in the ready queue.
    Queued,
    /// This flow is currently being polled, and we haven't heard back a feedback
    /// from the scheduler decision.
    Polling {
        /// This is there to capture inline wake notifications while the flow is being
        /// polled (and after it returned `Poll::Pending`). The flow is initially scheduled
        /// with `notified` set to `false`, and when the waker is woken up, it'll set it to
        /// `true`. When the scheduler hears back from the feedback on decision, it can put
        /// it in the ready queue even if the flow returned Poll::Pending.
        notified: bool,
    },
}

struct FlowState {
    /// Signed DRR deficit counter in bytes.
    ///
    /// Positive values are available service credits. Negative values are allowed
    /// and it means that the flow is currently in debit (because proposal sizes are
    /// charged after the polling decision). A flow with a negative deficit
    /// won't get polled again until its deficit goes positive again over the rounds.
    deficit: i64,
    state: State,
    in_flight: u64,
    reported_in_flight: Option<u64>,
    limit: u64,
    blocked_since: Option<Instant>,
    metrics: FlowMetrics,
}

struct FlowMetrics {
    in_flight: Gauge,
    blocked_ms: Counter,
    receive_to_propose: Histogram,
}

impl FlowState {
    fn report_window(&mut self, now: Instant) {
        if let Some(since) = &mut self.blocked_since {
            let millis = now.saturating_duration_since(*since).as_millis() as u64;
            if millis > 0 {
                self.metrics.blocked_ms.increment(millis);
                *since += Duration::from_millis(millis);
            }
        }
        if self.in_flight >= self.limit {
            self.blocked_since.get_or_insert(now);
        } else {
            self.blocked_since = None;
        }
        if self.reported_in_flight != Some(self.in_flight) {
            self.metrics.in_flight.set(self.in_flight as f64);
            self.reported_in_flight = Some(self.in_flight);
        }
    }
}

struct Inner {
    parent_waker: Option<Waker>,
    state: EnumMap<SelfProposerSchedulerFlow, FlowState>,
    ready_ring: VecDeque<SelfProposerSchedulerFlow>,
    new_proposals: EnumMap<SelfProposerSchedulerFlow, u64>,
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
    fn label(self) -> &'static str {
        match self {
            Self::Invoker => "invoker",
            Self::Timer => "timer",
            Self::Shuffle => "shuffle",
            Self::Cleaner => "cleaner",
            Self::UpsertSchema => "upsert-schema",
            Self::UpsertRuleBook => "upsert-rule-book",
            Self::NetworkService => "network-service",
            Self::PartitionMaintenance => "partition-maintenance",
            Self::Scheduler => "scheduler",
        }
    }

    fn limit(self, options: &WorkerOptions) -> u64 {
        let limits = &options.self_proposal_max_in_flight;
        let limit = match self {
            Self::Invoker => limits.invoker,
            Self::Timer => limits.timer,
            Self::Shuffle => limits.shuffle,
            Self::Cleaner => options.cleanup_max_in_flight_purges(),
            Self::UpsertSchema => limits.upsert_schema,
            Self::UpsertRuleBook => limits.upsert_rule_book,
            Self::NetworkService => limits.network_service,
            Self::PartitionMaintenance => limits.partition_maintenance,
            Self::Scheduler => limits.scheduler,
        };
        u64::from(limit.get())
    }

    /// Returns the cost multiplier for a flow. The multiplier is multiplied by the number of bytes proposed for that flow,
    /// resulting into it being charged more for those bytes. A higher multiplier biases the scheduler "against" that flow
    /// (i.e. deprioritizing it).
    fn cost_multiplier(&self) -> u32 {
        match self {
            // The cleaner messages are pretty small (just an invocation id), but are expensive to process.
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
}

/// The scheduler's decision of which flow to poll next. The caller must then poll the returned flow and
/// report back the results via methods on this struct. Failing to report back the results will result in a panic.
pub(crate) struct SchedulerDecision<'a> {
    /// The flow that's in turn to be polled according to the scheduler's decision.
    pub(crate) flow: SelfProposerSchedulerFlow,
    /// The flow specific waker to be passed to the flow's poll method.
    /// Once it's woken up, this flow moves to the ready queue.
    pub(crate) waker: &'a Waker,
    inner: &'a mut Arc<Mutex<Inner>>,
    received_feedback: bool,
}

impl<'a> SchedulerDecision<'a> {
    fn requeue(
        flow: SelfProposerSchedulerFlow,
        state: &mut FlowState,
        ready_ring: &mut VecDeque<SelfProposerSchedulerFlow>,
        force_back: bool,
    ) {
        if state.deficit > 0 && !force_back {
            ready_ring.push_front(flow);
        } else {
            ready_ring.push_back(flow);
        }
        state.state = State::Queued;
    }

    /// Report back how many bytes were written to the self-proposer as a result of handling
    /// the flow. A flow that managed to write something will be automatically considered
    /// ready to be polled again. Depending on its deficit, it may get re-enqueued at the
    /// head or the back of the ready queue.
    ///
    /// Note: Flows that report back 0 bytes written will lose their position in the
    /// ready queue to avoid starvation.
    /// `records` counts actual enqueues, independently of the byte charge used for
    /// fairness; discarded/noop work consumes no application-window credits.
    pub(crate) fn on_proposal_enqueued(
        mut self,
        bytes_written: usize,
        records: u64,
        received_at: Option<Instant>,
    ) {
        self.received_feedback = true;
        let mut inner = self.inner.lock();
        let Inner {
            state,
            ready_ring,
            new_proposals,
            ..
        } = &mut *inner;
        let state = &mut state[self.flow];
        state.in_flight += records;
        new_proposals[self.flow] += records;
        if state.in_flight >= state.limit && state.blocked_since.is_none() {
            state.blocked_since = Some(Instant::now());
        }
        if records > 0
            && let Some(received_at) = received_at
        {
            state
                .metrics
                .receive_to_propose
                .record(received_at.elapsed());
        }
        state.deficit = state.deficit.saturating_sub(
            bytes_written.saturating_mul(self.flow.cost_multiplier() as usize) as i64,
        );
        std::debug_assert_matches!(state.state, State::Polling { .. });
        Self::requeue(self.flow, state, ready_ring, bytes_written == 0);
    }

    /// Reports back that the flow was ready, but reported an error without proposing any bytes.
    /// Will be treated as an empty proposal (see [`Self::on_proposal_enqueued`]).
    pub(crate) fn on_error(self) {
        self.on_proposal_enqueued(0, 0, None)
    }

    /// Reports back to the scheduler that the flow was polled and reported `Poll::Pending`.
    /// The flow won't get polled again unless it calls `Waker::wake` on the waker that was passed
    /// to its poll method.
    pub(crate) fn on_pending(mut self) {
        self.received_feedback = true;
        let mut inner = self.inner.lock();
        let Inner {
            state, ready_ring, ..
        } = &mut *inner;
        let state = &mut state[self.flow];
        let State::Polling { notified } = &mut state.state else {
            panic!("state is not polling, this is a bug");
        };
        if *notified {
            // To avoid starvation, we're going to move the flow back to the back of the ready queue.
            Self::requeue(self.flow, state, ready_ring, /* force back */ true);
        } else if state.deficit >= 0 {
            // We're truly pending, reset the deficit and move on.
            state.state = State::Pending;
            state.deficit = 0;
        } else {
            // The stream is in debit, it should stay in the ready queue until the debit is paid.
            Self::requeue(self.flow, state, ready_ring, /* force back */ true);
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
        let mut guard = self.inner.lock();
        let Inner {
            state, ready_ring, ..
        } = &mut *guard;
        let state = &mut state[self.flow];
        // Steal its deficit if it's positive so that it can get added at the back of the ready ring.
        state.deficit = state.deficit.min(0);
        Self::requeue(self.flow, state, ready_ring, /* force back */ true);
    }
}

/// A scheduler that sits in front of the self-proposer to decide which one of the possible self-proposer writers
/// should be allowed to write next. This is a byte-aware DRR scheduler that tries to ensure fairness among the writers
/// based on the amount of bytes each one have proposed.
///
/// Because the scheduler doesn't know the cost of a flow beforehand, flows report back their sizes after getting polled.
/// This means that we allow flows to go into negative deficits. A flow with a negative deficit won't get polled again
/// until its deficit goes positive again over the rounds. Compared to typical DRR schedulers, flows with negative deficits
/// will get their deficit refilled even if they're no longer ready.
pub(crate) struct SelfProposerScheduler {
    inner: Arc<Mutex<Inner>>,
    /// A cache for the per-flow wakers
    wakers: EnumMap<SelfProposerSchedulerFlow, Waker>,
    pending: VecDeque<ProposalBatch>,
    pending_invoker: Gauge,
    pending_network: Gauge,
}

struct ProposalBatch {
    commit: CommitToken,
    committed_lsn: Option<Lsn>,
    records: EnumMap<SelfProposerSchedulerFlow, u64>,
}

impl SelfProposerScheduler {
    pub(crate) fn new(partition_id: PartitionId) -> SelfProposerScheduler {
        let partition = partition_id.to_string();
        let state = EnumMap::from_fn(|flow: SelfProposerSchedulerFlow| FlowState {
            deficit: 0,
            state: State::Queued,
            in_flight: 0,
            reported_in_flight: None,
            limit: u64::MAX,
            blocked_since: None,
            metrics: FlowMetrics {
                in_flight: gauge!(SELF_PROPOSER_INFLIGHT, PARTITION_LABEL => partition.clone(), "flow" => flow.label()),
                blocked_ms: counter!(SELF_PROPOSER_WINDOW_BLOCKED_MS, PARTITION_LABEL => partition.clone(), "flow" => flow.label()),
                receive_to_propose: histogram!(SELF_PROPOSER_RECEIVE_TO_PROPOSE, PARTITION_LABEL => partition.clone(), "flow" => flow.label()),
            },
        });
        let inner = Arc::new(Mutex::new(Inner {
            // The current waker will be set on the first poll and keep getting updated there.
            parent_waker: None,
            // All flows must be ready once on creation so that we can poll them immediately
            // and register their wakers.
            ready_ring: VecDeque::from_iter(state.iter().map(|(flow, _)| flow)),
            state,
            new_proposals: EnumMap::default(),
        }));
        let wakers = EnumMap::from_fn(|flow| {
            Waker::from(Arc::new(FlowWaker {
                flow,
                inner: Arc::clone(&inner),
            }))
        });
        Self {
            inner,
            wakers,
            pending: VecDeque::new(),
            pending_invoker: gauge!(SELF_PROPOSER_PENDING, PARTITION_LABEL => partition.clone(), "flow" => "invoker"),
            pending_network: gauge!(SELF_PROPOSER_PENDING, PARTITION_LABEL => partition, "flow" => "network-service"),
        }
    }

    pub fn report_pending(&self, invoker: usize, network: usize) {
        self.pending_invoker.set(invoker as f64);
        self.pending_network.set(network as f64);
    }

    /// One receipt per admission round, not per record. Receipts are ordered because
    /// the single background appender preserves enqueue order.
    pub fn track_proposals(&mut self, commit: CommitToken) {
        let records = std::mem::take(&mut self.inner.lock().new_proposals);
        debug_assert!(records.values().any(|count| *count > 0));
        self.pending.push_back(ProposalBatch {
            commit,
            committed_lsn: None,
            records,
        });
    }

    /// The partition loop polls this again after committing each application batch.
    /// Bifrost commit alone never returns admission credits. The receipt's LSN may
    /// conservatively include later records in the same appender batch.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        applied_lsn: Lsn,
        options: &WorkerOptions,
    ) -> Result<(), Error> {
        while let Some(batch) = self.pending.front_mut() {
            let lsn = match batch.committed_lsn {
                Some(lsn) => lsn,
                None => match batch.commit.poll_unpin(cx) {
                    Poll::Ready(Ok(lsn)) => {
                        batch.committed_lsn = Some(lsn);
                        lsn
                    }
                    Poll::Ready(Err(err)) => return Err(Error::task_failed("self-proposer", err)),
                    Poll::Pending => break,
                },
            };
            if lsn > applied_lsn {
                break;
            }
            let batch = self.pending.pop_front().expect("front exists");
            self.release_applied(batch.records);
        }
        let now = Instant::now();
        for (flow, state) in &mut self.inner.lock().state {
            state.limit = flow.limit(options);
            state.report_window(now);
        }
        Ok(())
    }

    fn release_applied(&mut self, records: EnumMap<SelfProposerSchedulerFlow, u64>) {
        let mut inner = self.inner.lock();
        for (flow, count) in records {
            inner.state[flow].in_flight -= count;
        }
    }

    /// Polls the scheduler for the next decision. The returned [`SchedulerDecision`] is a handle to the flow that's
    /// scheduled for polling. The caller then should poll the flow inside the decision while passing the associated waker.
    /// The caller MUST report back the result of the poll (via the methods of [`SchedulerDecision`]), dropping the decision
    /// without doing so will result in a panic.
    /// This function returns None if there are currently no flows that are ready to be polled.
    #[must_use]
    pub fn poll_next_ready(&mut self, cx: &mut Context<'_>) -> Option<SchedulerDecision<'_>> {
        let mut guard = self.inner.lock();
        let Inner {
            state,
            ready_ring,
            parent_waker,
            ..
        } = &mut *guard;

        match parent_waker {
            Some(w) => w.clone_from(cx.waker()),
            None => *parent_waker = Some(cx.waker().clone()),
        }

        loop {
            let round_length = ready_ring.len();
            if round_length == 0 {
                return None;
            }

            let mut closest_deficit = i64::MIN;
            let mut eligible = false;
            for _ in 0..round_length {
                let flow = ready_ring
                    .pop_front()
                    .expect("guarded by the round_length check earlier");
                let state = &mut state[flow];
                std::debug_assert_matches!(state.state, State::Queued);

                if state.in_flight >= state.limit {
                    ready_ring.push_back(flow);
                    continue;
                }
                eligible = true;

                if state.deficit <= 0 {
                    state.deficit += QUANTUM;
                }

                if state.deficit > 0 {
                    state.state = State::Polling { notified: false };
                    drop(guard);
                    return Some(SchedulerDecision {
                        flow,
                        waker: &self.wakers[flow],
                        inner: &mut self.inner,
                        received_feedback: false,
                    });
                } else {
                    ready_ring.push_back(flow);
                    closest_deficit = closest_deficit.max(state.deficit);
                }
            }

            if !eligible {
                return None;
            }
            // All eligible flows are in debit; full windows do not accrue credits.
            // As an optimization, let's fast forward the rounds until we find an eligible flow.
            // To do that we:
            //   - Find the flow with the deficit closest to zero.
            //   - Calculate how many rounds we need to take to positive.
            //   - Fast forward every flow by the estimated number of rounds.
            assert!(closest_deficit <= 0);
            let rounds_to_skip = closest_deficit.saturating_abs() / QUANTUM;
            ready_ring.iter_mut().for_each(|flow| {
                if state[*flow].in_flight < state[*flow].limit {
                    state[*flow].deficit += rounds_to_skip * QUANTUM;
                }
            });
        }
    }
}

impl Drop for SelfProposerScheduler {
    fn drop(&mut self) {
        let now = Instant::now();
        for state in self.inner.lock().state.values_mut() {
            state.in_flight = 0;
            state.report_window(now);
        }
        self.report_pending(0, 0);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Wake, Waker};

    use enum_map::EnumMap;

    use restate_types::config::WorkerOptions;
    use restate_types::identifiers::PartitionId;
    use restate_types::logs::{Lsn, SequenceNumber};

    use crate::partition::leadership::self_proposer_scheduler::{
        QUANTUM, SelfProposerSchedulerFlow,
    };

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

        let mut scheduler =
            super::SelfProposerScheduler::new(restate_types::identifiers::PartitionId::MIN);

        // First poll, expecting the invoker flow.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);

        // Report back 1KB of commands written
        dec.on_proposal_enqueued(1024, 0, None);
        // Invoker's deficit should still be positive, so asking the scheduler again should yield it again.
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
        // Report back 128KB of commands, to consume the entire deficit of the invoker.
        dec.on_proposal_enqueued(128 * 1024, 0, None);

        // The invoker is the only queued flow, it should get its deficit back.
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

        let mut scheduler =
            super::SelfProposerScheduler::new(restate_types::identifiers::PartitionId::MIN);

        // Report 100x the quantum for the invoker
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        dec.on_proposal_enqueued(100 * QUANTUM as usize, 0, None);

        // Report 50x the quantum for the timer
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_proposal_enqueued(50 * QUANTUM as usize, 0, None);

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

    #[tokio::test(start_paused = true)]
    async fn full_window_preserves_rpc_capacity_and_reports_blocked_time() {
        use std::sync::atomic::AtomicU64;
        use std::time::Duration;

        #[derive(Default)]
        struct Total(AtomicU64);
        impl metrics::CounterFn for Total {
            fn increment(&self, value: u64) {
                self.0.fetch_add(value, Ordering::Relaxed);
            }
            fn absolute(&self, value: u64) {
                self.0.fetch_max(value, Ordering::Relaxed);
            }
        }

        #[derive(Default)]
        struct Samples(std::sync::Mutex<Vec<f64>>);
        impl metrics::HistogramFn for Samples {
            fn record(&self, value: f64) {
                self.0.lock().unwrap().push(value);
            }
        }

        use SelfProposerSchedulerFlow as Flow;
        let mut options = WorkerOptions::default();
        options.self_proposal_max_in_flight.invoker = 2.try_into().unwrap();
        let mut scheduler = super::SelfProposerScheduler::new(PartitionId::MIN);
        let total = Arc::new(Total::default());
        let samples = Arc::new(Samples::default());
        scheduler.inner.lock().state[Flow::Invoker]
            .metrics
            .blocked_ms = metrics::Counter::from_arc(total.clone());
        scheduler.inner.lock().state[Flow::Invoker]
            .metrics
            .receive_to_propose = metrics::Histogram::from_arc(samples.clone());
        let mut cx = Context::from_waker(Waker::noop());
        scheduler
            .poll_progress(&mut cx, Lsn::INVALID, &options)
            .unwrap();
        let decision = scheduler.poll_next_ready(&mut cx).unwrap();
        assert_eq!(decision.flow, Flow::Invoker);
        let invoker_waker = decision.waker.clone();
        decision.on_proposal_enqueued(
            1024,
            2,
            Some(tokio::time::Instant::now() - Duration::from_secs(2)),
        );
        invoker_waker.wake_by_ref();

        // Full invoker windows cannot be bypassed by wakeups. Other flows, including
        // RPCs, remain selectable; exhaustion returns Pending instead of spinning.
        let mut saw_network = false;
        while let Some(decision) = scheduler.poll_next_ready(&mut cx) {
            assert_ne!(decision.flow, Flow::Invoker);
            saw_network |= decision.flow == Flow::NetworkService;
            decision.on_pending();
        }
        assert!(saw_network);
        tokio::time::advance(Duration::from_millis(1500)).await;
        scheduler
            .poll_progress(&mut cx, Lsn::INVALID, &options)
            .unwrap();
        assert_eq!(total.0.load(Ordering::Relaxed), 1500);
        scheduler
            .poll_progress(&mut cx, Lsn::INVALID, &options)
            .unwrap();
        assert_eq!(total.0.load(Ordering::Relaxed), 1500);

        // Raising the limit is live. A noop does not consume a slot, and a formed
        // batch can overdraw the window once, but cannot admit another batch.
        options.self_proposal_max_in_flight.invoker = 3.try_into().unwrap();
        scheduler
            .poll_progress(&mut cx, Lsn::INVALID, &options)
            .unwrap();
        scheduler
            .poll_next_ready(&mut cx)
            .unwrap()
            .on_proposal_enqueued(1024, 0, Some(tokio::time::Instant::now()));
        scheduler
            .poll_next_ready(&mut cx)
            .unwrap()
            .on_proposal_enqueued(1024, 4, None);
        assert!(scheduler.poll_next_ready(&mut cx).is_none());
        assert_eq!(scheduler.inner.lock().state[Flow::Invoker].in_flight, 6);

        let mut released = EnumMap::default();
        released[Flow::Invoker] = 6;
        scheduler.release_applied(released);
        scheduler
            .poll_progress(&mut cx, Lsn::INVALID, &options)
            .unwrap();
        assert_eq!(
            scheduler.poll_next_ready(&mut cx).map(|d| {
                let flow = d.flow;
                d.on_pending();
                flow
            }),
            Some(Flow::Invoker)
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        scheduler
            .poll_progress(&mut cx, Lsn::INVALID, &options)
            .unwrap();
        assert_eq!(total.0.load(Ordering::Relaxed), 1500);
        assert_eq!(*samples.0.lock().unwrap(), vec![2.0]);
    }

    #[restate_core::test]
    async fn credits_wait_for_application_after_bifrost_commit() -> anyhow::Result<()> {
        use restate_bifrost::{Bifrost, ErrorRecoveryStrategy};
        use restate_core::TestCoreEnv;
        use restate_types::logs::LogId;

        use SelfProposerSchedulerFlow as Flow;
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;
        let mut appender = bifrost
            .create_background_appender::<String>(
                LogId::new(0),
                ErrorRecoveryStrategy::Wait,
                None,
                10,
            )?
            .start("proposal-window-test")?;
        let mut scheduler = super::SelfProposerScheduler::new(PartitionId::MIN);
        let mut options = WorkerOptions::default();
        options.self_proposal_max_in_flight.invoker = 2.try_into().unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        scheduler.poll_progress(&mut cx, Lsn::INVALID, &options)?;
        let decision = scheduler.poll_next_ready(&mut cx).unwrap();
        assert_eq!(decision.flow, Flow::Invoker);
        let sender = appender.sender();
        let bytes = sender.enqueue("one".to_owned())? + sender.enqueue("two".to_owned())?;
        decision.on_proposal_enqueued(bytes, 2, None);
        scheduler.track_proposals(sender.notify_committed()?);
        let committed = sender.notify_committed()?.await?;
        assert_eq!(committed, Lsn::from(2u64));

        scheduler.poll_progress(&mut cx, committed.prev(), &options)?;
        assert_eq!(scheduler.inner.lock().state[Flow::Invoker].in_flight, 2);
        while let Some(decision) = scheduler.poll_next_ready(&mut cx) {
            assert_ne!(decision.flow, Flow::Invoker);
            decision.on_pending();
        }
        scheduler.poll_progress(&mut cx, committed, &options)?;
        assert_eq!(scheduler.inner.lock().state[Flow::Invoker].in_flight, 0);
        assert!(scheduler.pending.is_empty());
        let decision = scheduler.poll_next_ready(&mut cx).unwrap();
        assert_eq!(decision.flow, Flow::Invoker);
        decision.on_pending();
        appender.drain().await?;
        Ok(())
    }
}
