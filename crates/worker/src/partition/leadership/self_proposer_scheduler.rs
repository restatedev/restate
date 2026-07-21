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
use std::task::{Context, Wake, Waker};

use enum_map::{Enum, EnumMap};

use restate_platform::sync::Mutex;
use tracing::warn;

const QUANTUM: i64 = 64 * 1024; // 64KB

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

#[derive(Debug, Clone)]
struct FlowState {
    /// Signed DRR deficit counter in bytes.
    ///
    /// Positive values are available service credits. Negantive values are allowed
    /// and it means that the flow is currently in debit (because proposal sizes are
    /// chared after the polling decision). A flow with a negative deficit
    /// won't get polled again until its deficit goes positive again over the rounds.
    deficit: i64,
    state: State,
}

struct Inner {
    parent_waker: Option<Waker>,
    state: EnumMap<SelfProposerSchedulerFlow, FlowState>,
    ready_ring: VecDeque<SelfProposerSchedulerFlow>,
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

/// The scheduler's decision of which flow to poll next. The caller must then poll the returned flow and
/// report back the results via mehtods on this struct. Failing to report back the results will result in a panic.
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
    /// ready to be polled again. Depepdning on its deficit, it may get re-enqueued at the
    /// head or the back of the ready queue.
    ///
    /// Note: Flows that doesn't reports back 0 bytes written will lose their position in the
    /// ready queue to avoid starvation.
    pub(crate) fn on_proposal_enqueued(mut self, bytes_written: usize) {
        self.received_feedback = true;
        let mut inner = self.inner.lock();
        let Inner {
            state, ready_ring, ..
        } = &mut *inner;
        let state = &mut state[self.flow];
        state.deficit -= bytes_written as i64;
        std::debug_assert_matches!(state.state, State::Polling { .. });
        Self::requeue(self.flow, state, ready_ring, bytes_written == 0);
    }

    /// Reports back that the flow was ready, but reported an error without proposing any bytes.
    /// Will be treated as an empty proposal (see [`SelfProposerScheduler::on_proposal_enqueued`]).
    pub(crate) fn on_error(mut self) {
        self.received_feedback = true;
        self.on_proposal_enqueued(0)
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
            Self::requeue(self.flow, state, ready_ring, /* force back */ false);
        } else {
            // We're truly pending, reset the deficit and move on.
            state.state = State::Pending;
            state.deficit = 0;
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
/// This means that we we allow flows to go into negative deficits. A flow with a negative deficit won't get polled again
/// until its deficit goes positive again over the rounds. Compared to typical DRR schedulers, flows with negative deficits
/// will get their deficit refilled even if they're no longer ready.
pub(crate) struct SelfProposerScheduler {
    inner: Arc<Mutex<Inner>>,
    /// A cache for the per-flow wakers
    wakers: EnumMap<SelfProposerSchedulerFlow, Waker>,
}

impl Default for SelfProposerScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl SelfProposerScheduler {
    pub(crate) fn new() -> SelfProposerScheduler {
        let state = EnumMap::from_fn(|_| FlowState {
            deficit: 0,
            state: State::Queued,
        });
        let inner = Arc::new(Mutex::new(Inner {
            // The current waker will be set on the first poll and keep getting updated there.
            parent_waker: None,
            // All flows must be ready once on creation so that we can poll them immediately
            // and register their wakers.
            ready_ring: VecDeque::from_iter(state.iter().map(|(flow, _)| flow)),
            state,
        }));
        let wakers = EnumMap::from_fn(|flow| {
            Waker::from(Arc::new(FlowWaker {
                flow,
                inner: Arc::clone(&inner),
            }))
        });
        Self { inner, wakers }
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
        } = &mut *guard;

        match parent_waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => *parent_waker = Some(cx.waker().clone()),
        }

        loop {
            let round_length = ready_ring.len();
            if round_length == 0 {
                return None;
            }

            for _ in 0..round_length {
                let flow = ready_ring
                    .pop_front()
                    .expect("guarded by the round_length check earlier");
                let state = &mut state[flow];
                std::debug_assert_matches!(state.state, State::Queued);

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
                }
            }

            // We did one full round and no flows were returned. It means that all flows are in debit.
            // As an optimization, let's fast forward the rounds until we find an eligible flow.
            // To do that we:
            //   - Find the flow with the deficit closest to zero.
            //   - Calculate how many rounds we need to take to positive.
            //   - Fast forward every flow by the estimated number of rounds.
            let closest_deficit = ready_ring
                .iter()
                .map(|flow| state[*flow].deficit)
                .max()
                .expect("at least one flow");
            assert!(closest_deficit <= 0);
            let rounds_to_skip = closest_deficit.saturating_abs() / QUANTUM;
            ready_ring.iter_mut().for_each(|flow| {
                state[*flow].deficit += rounds_to_skip * QUANTUM;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Wake, Waker};

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

        let mut scheduler = super::SelfProposerScheduler::new();

        // First poll, expecting the invoker flow.
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);

        // Report back 1KB of commands written
        dec.on_proposal_enqueued(1024);
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
        // Reporting pending now, should still re-enqueue the flow given the inline wake.
        dec.on_pending();
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        // Report back 128KB of commands, to consume the entire deficit of the invoker.
        dec.on_proposal_enqueued(128 * 1024);

        // Now we should get the next flow
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

        // We've consumed all the flows, now the invoker should get its deficit back.
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

        let mut scheduler = super::SelfProposerScheduler::new();

        // Report 100x the quantum for the invoker
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Invoker);
        dec.on_proposal_enqueued(100 * QUANTUM as usize);

        // Report 50x the quantum for the timer
        let dec = scheduler
            .poll_next_ready(&mut cx)
            .expect("expected flow, got none");
        assert_eq!(dec.flow, SelfProposerSchedulerFlow::Timer);
        dec.on_proposal_enqueued(50 * QUANTUM as usize);

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
}
