// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::Poll;

use restate_futures_util::concurrency::{Concurrency, Permit};

use super::grouped_waiters::GroupedWaiters;
use crate::scheduler::VQueueHandle;
use crate::scheduler::eligible::{SchedulingGroup, WeightResolver};

pub struct InvokerConcurrencyLimiter {
    limiter: Concurrency,
    // Weighted round-robin over service groups instead of a flat FIFO: with a
    // FIFO, whichever service floods the waiter list first monopolizes freed
    // permits and starves every other service regardless of scheduling weights.
    waiters: GroupedWaiters,
    cached_permit: Permit,
}

impl InvokerConcurrencyLimiter {
    pub fn new(limiter: Concurrency, weight_resolver: WeightResolver) -> Self {
        Self {
            limiter,
            waiters: GroupedWaiters::new(weight_resolver),
            cached_permit: Permit::new_empty(),
        }
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.waiters.remove(vqueue);
    }

    /// Attempts to claim a permit for a queue the scheduler decided to dispatch.
    ///
    /// Unlike the previous FIFO design, this does NOT gate the claim on the
    /// caller being the waiter-list head: the eligibility ring already applies
    /// weighted round-robin when choosing which queue polls, and with a
    /// rotating (WRR) waiter head the head-only gate livelocks — the woken
    /// queue arrives at dispatch after the head has rotated past it, fails,
    /// re-queues, and the freed permit is never claimed. The waiter list's job
    /// is reduced to picking the wake-up order (see `poll_head`), which is
    /// where the group weights apply.
    pub(super) fn poll_acquire(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
        group: &SchedulingGroup,
    ) -> Option<Permit> {
        // cached permit exists (set aside by poll_head when it woke a waiter)
        if let Some(permit) = self.cached_permit.split(1) {
            self.claimed(cx, vqueue);
            return Some(permit);
        }

        if let Poll::Ready(invoker_permit) = self.limiter.poll_acquire(cx) {
            self.claimed(cx, vqueue);
            return Some(invoker_permit);
        }

        // No permit available: park this queue in its service group's wait list
        // (push_back de-duplicates).
        self.waiters.push_back(vqueue, group);
        None
    }

    fn claimed(&mut self, cx: &mut std::task::Context<'_>, vqueue: VQueueHandle) {
        self.waiters.remove(vqueue);
        if !self.waiters.is_empty() {
            cx.waker().wake_by_ref();
        }
    }

    pub fn poll_head(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<VQueueHandle>> {
        if self.waiters.is_empty() {
            return Poll::Ready(None);
        }

        tracing::trace!(
            "Polling invoker concurrency permits: {} waiters. Cached permit: {:?}",
            self.waiters.len(),
            self.cached_permit
        );

        match self.limiter.poll_acquire(cx) {
            Poll::Ready(permit) => {
                self.cached_permit.merge(permit);
                tracing::trace!("MERGED NEW PERMIT, CURRENT: {:?}", self.cached_permit);
            }
            Poll::Pending => {}
        }

        if !self.cached_permit.is_empty() {
            // store this permit for the next poller.
            let vqueue = self.waiters.pop_front().unwrap();
            if !self.waiters.is_empty() {
                // make sure to take the waker again for the next poll
                cx.waker().wake_by_ref();
            }
            return Poll::Ready(Some(vqueue));
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;

    use slotmap::SlotMap;

    use restate_types::ServiceName;

    use super::*;
    use crate::scheduler::eligible::WeightResolver;

    fn resolver() -> WeightResolver {
        Arc::new(|_: &SchedulingGroup| NonZeroU32::MIN)
    }

    fn group(name: &str) -> SchedulingGroup {
        SchedulingGroup::Service(ServiceName::new(name))
    }

    /// The wake-then-steal race is intentional and livelock-free: `poll_head`
    /// sets a permit aside and wakes waiter A, but whichever queue the
    /// scheduler dispatches first may claim it. The loser simply re-parks and
    /// the WRR waiter list routes a later permit to it — no permit is ever
    /// stranded (the livelock the previous head-gated design had).
    #[test]
    fn woken_permit_can_be_claimed_by_another_queue_without_stranding() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let vq_a = handles.insert(());
        let vq_b = handles.insert(());
        let group_a = group("a");
        let group_b = group("b");

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut limiter = InvokerConcurrencyLimiter::new(
            Concurrency::new(Some(NonZeroUsize::new(1).unwrap())),
            resolver(),
        );

        // holder takes the only permit; A and B park in the waiter list
        let permit = limiter
            .poll_acquire(&mut cx, holder, &group("holder"))
            .expect("permit");
        assert!(limiter.poll_acquire(&mut cx, vq_a, &group_a).is_none());
        assert!(limiter.poll_acquire(&mut cx, vq_b, &group_b).is_none());

        // release; poll_head caches the freed permit and wakes A (WRR head)
        drop(permit);
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));

        // B "wins the race" to dispatch first and steals the cached permit
        let stolen = limiter
            .poll_acquire(&mut cx, vq_b, &group_b)
            .expect("B claims the cached permit");

        // A loses, re-parks — and the next released permit reaches A: nothing
        // is stranded and the loser is not starved
        assert!(limiter.poll_acquire(&mut cx, vq_a, &group_a).is_none());
        drop(stolen);
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));
        assert!(
            limiter.poll_acquire(&mut cx, vq_a, &group_a).is_some(),
            "A claims the permit on its wake"
        );
    }
}
