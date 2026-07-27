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

use slotmap::SecondaryMap;

use restate_futures_util::concurrency::{Concurrency, Permit};
use restate_types::ServiceName;

use super::grouped_waiters::GroupedWaiters;
use crate::scheduler::VQueueHandle;
use crate::scheduler::eligible::{LaneWeightResolver, SchedulingGroup, WeightResolver};

pub struct InvokerConcurrencyLimiter {
    limiter: Concurrency,
    // Weighted round-robin over service groups instead of a flat FIFO, so no
    // service can monopolize freed permits regardless of arrival order.
    waiters: GroupedWaiters,
    cached_permit: Permit,
    /// Queues `poll_head` woke for a cached permit that have not claimed one
    /// yet. A queue that loses the wake-then-steal race re-parks at the front
    /// of its own lane with its stride refunded. Cleared on successful claim
    /// and on external removal, so a stale flag can never grant a perpetual
    /// front position.
    woken: SecondaryMap<VQueueHandle, ()>,
}

impl InvokerConcurrencyLimiter {
    pub fn new(
        limiter: Concurrency,
        weight_resolver: WeightResolver,
        lane_weight_resolver: LaneWeightResolver,
    ) -> Self {
        Self {
            limiter,
            waiters: GroupedWaiters::new(weight_resolver, lane_weight_resolver),
            cached_permit: Permit::new_empty(),
            woken: SecondaryMap::new(),
        }
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.woken.remove(vqueue);
        self.waiters.remove(vqueue);
    }

    /// Attempts to claim a permit for a queue the scheduler decided to dispatch.
    ///
    /// This does not gate the claim on the caller being the waiter-list head:
    /// with a rotating WRR waiter head, a head-only gate livelocks (the woken
    /// queue arrives after the head has rotated past it). The waiter list's
    /// job is reduced to picking the wake-up order (see `poll_head`).
    pub(super) fn poll_acquire(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
        group: &SchedulingGroup,
        service: &ServiceName,
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

        // No permit available: park this queue in its service lane. A queue
        // that lost the wake-then-steal race keeps its turn at the front.
        if self.woken.remove(vqueue).is_some() {
            self.waiters.push_front(vqueue, group, service);
        } else {
            self.waiters.push_back(vqueue, group, service);
        }
        None
    }

    fn claimed(&mut self, cx: &mut std::task::Context<'_>, vqueue: VQueueHandle) {
        self.woken.remove(vqueue);
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
            // remember the chosen waiter: if it loses the claim race it keeps
            // its turn (front of its lane, stride refunded) instead of
            // re-parking at the back
            self.woken.insert(vqueue, ());
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

    fn lane_resolver() -> LaneWeightResolver {
        Arc::new(|_: &SchedulingGroup, _: &ServiceName| NonZeroU32::MIN)
    }

    fn group(name: &str) -> SchedulingGroup {
        SchedulingGroup::Service(ServiceName::new(name))
    }

    fn svc(name: &str) -> ServiceName {
        ServiceName::new(name)
    }

    fn limiter_with_one_permit() -> InvokerConcurrencyLimiter {
        InvokerConcurrencyLimiter::new(
            Concurrency::new(Some(NonZeroUsize::new(1).unwrap())),
            resolver(),
            lane_resolver(),
        )
    }

    /// The wake-then-steal race is intentional and livelock-free: `poll_head`
    /// sets a permit aside and wakes waiter A, but whichever queue the
    /// scheduler dispatches first may claim it. The loser keeps its turn
    /// (front of its own lane, stride refunded), so no permit is ever
    /// stranded and the loser is never demoted to the back of the group.
    #[test]
    fn woken_permit_can_be_claimed_by_another_queue_without_stranding() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let vq_a = handles.insert(());
        let vq_a2 = handles.insert(());
        let vq_b = handles.insert(());
        let group_a = group("a");
        let group_b = group("b");
        let svc_a = svc("a");
        let svc_b = svc("b");

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut limiter = limiter_with_one_permit();

        // holder takes the only permit; A, A2 (same lane) and B park
        let permit = limiter
            .poll_acquire(&mut cx, holder, &group("holder"), &svc("holder"))
            .expect("permit");
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a)
                .is_none()
        );
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a2, &group_a, &svc_a)
                .is_none()
        );
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_b, &group_b, &svc_b)
                .is_none()
        );

        // release; poll_head caches the freed permit and wakes A (WRR head)
        drop(permit);
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));

        // B "wins the race" to dispatch first and steals the cached permit
        let stolen = limiter
            .poll_acquire(&mut cx, vq_b, &group_b, &svc_b)
            .expect("B claims the cached permit");

        // A loses and re-parks — at the FRONT of its lane, ahead of A2
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a)
                .is_none()
        );
        assert_eq!(
            limiter.waiters.front(),
            Some(vq_a),
            "steal loser keeps its turn at the front of its lane"
        );

        // the next released permit reaches A, not A2 and not B's lane again
        drop(stolen);
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a)
                .is_some(),
            "A claims the permit on its wake"
        );
    }

    /// A stale `woken` flag must not grant a perpetual front position: after
    /// external removal (dormancy path), a later re-park is a plain push_back.
    #[test]
    fn stale_woken_flag_cleared_on_removal() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let vq_a = handles.insert(());
        let vq_a2 = handles.insert(());
        let group_a = group("a");
        let svc_a = svc("a");

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut limiter = limiter_with_one_permit();

        let permit = limiter
            .poll_acquire(&mut cx, holder, &group("holder"), &svc("holder"))
            .expect("permit");
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a)
                .is_none()
        );
        drop(permit);
        // A is woken (flag set)…
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));
        // …but goes dormant instead of claiming: the flag must be cleared
        limiter.remove_from_waiters(vq_a);
        // consume the cached permit so the re-parks below actually park
        let p = limiter
            .poll_acquire(&mut cx, holder, &group("holder"), &svc("holder"))
            .expect("cached permit");

        // A2 parks first, then A re-parks: A must land BEHIND A2 (plain
        // push_back — no front privilege from the stale flag)
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a2, &group_a, &svc_a)
                .is_none()
        );
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a)
                .is_none()
        );
        assert_eq!(
            limiter.waiters.front(),
            Some(vq_a2),
            "stale woken flag must not jump the queue"
        );
        drop(p);
    }
}
