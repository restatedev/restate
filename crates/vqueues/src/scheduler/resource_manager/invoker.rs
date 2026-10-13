// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::{Poll, Waker, ready};

use restate_futures_util::concurrency::{Concurrency, Permit};

use super::Waiters;
use crate::scheduler::VQueueHandle;

pub struct InvokerConcurrencyLimiter {
    limiter: Concurrency,
    waiters: Waiters,
    /// At most one queue has been notified, and its permit cannot be taken by another queue.
    notified: Option<(VQueueHandle, Permit)>,
    waker: Waker,
}

impl InvokerConcurrencyLimiter {
    pub fn new(limiter: Concurrency) -> Self {
        Self {
            limiter,
            waiters: Default::default(),
            notified: None,
            waker: Waker::noop().clone(),
        }
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        if self.cancel_notification(vqueue) {
            return;
        }

        if self.waiters.front() == Some(&vqueue) {
            self.waiters.pop_front();
            self.wake_next();
        } else {
            self.waiters.retain(|h| *h != vqueue);
        }

        if self.waiters.is_empty() {
            // Cloning resets PollSemaphore's pending acquisition. Otherwise it could reserve
            // capacity after the last waiter disappeared, keeping it from other partitions.
            self.limiter = self.limiter.clone();
        }
    }

    /// A notified queue is already on the ready ring, so it is no longer marked blocked.
    /// Cancel its reservation when its head changes or the queue becomes dormant.
    pub(super) fn cancel_notification(&mut self, vqueue: VQueueHandle) -> bool {
        if self
            .notified
            .as_ref()
            .is_some_and(|(head, _)| *head == vqueue)
        {
            drop(self.notified.take());
            self.wake_next();
            true
        } else {
            false
        }
    }

    fn wake_next(&self) {
        if !self.waiters.is_empty() {
            self.waker.wake_by_ref();
        }
    }

    pub(super) fn poll_acquire(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
    ) -> Option<Permit> {
        self.waker.clone_from(cx.waker());

        if self
            .notified
            .as_ref()
            .is_some_and(|(head, _)| *head == vqueue)
        {
            let (_, permit) = self.notified.take().unwrap();
            self.wake_next();
            return Some(permit);
        }

        // New arrivals must not bypass queues already waiting for capacity.
        if self.notified.is_none()
            && self.waiters.front().is_none_or(|head| *head == vqueue)
            && let Poll::Ready(permit) = self.limiter.poll_acquire(cx)
        {
            self.waiters.pop_front();
            self.wake_next();
            return Some(permit);
        }

        // Blocked queues are only retried after notification or removal from this wait-list.
        if self.waiters.front() != Some(&vqueue) {
            self.waiters.push_back(vqueue);
        }
        None
    }

    pub fn poll_head(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<VQueueHandle>> {
        self.waker.clone_from(cx.waker());
        if self.notified.is_some() {
            // Do not wake more queues or prefetch capacity until this permit is consumed.
            return Poll::Pending;
        }

        let Some(&vqueue) = self.waiters.front() else {
            return Poll::Ready(None);
        };

        let permit = ready!(self.limiter.poll_acquire(cx));
        self.waiters.pop_front();
        self.notified = Some((vqueue, permit));
        Poll::Ready(Some(vqueue))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Wake};

    use slotmap::SlotMap;

    use super::*;

    struct TestWaker(AtomicBool);

    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn released_capacity_notifies_one_waiter_without_prefetching_or_bypassing() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let waiters: Vec<_> = (0..100_000).map(|_| handles.insert(())).collect();
        let newcomer = handles.insert(());
        let mut cx = Context::from_waker(Waker::noop());
        let mut capacity = Concurrency::new(NonZeroUsize::new(2));
        let Poll::Ready(first) = capacity.poll_acquire(&mut cx) else {
            panic!("initial capacity is available");
        };
        let Poll::Ready(second) = capacity.poll_acquire(&mut cx) else {
            panic!("initial capacity is available");
        };
        let mut limiter = InvokerConcurrencyLimiter::new(capacity.clone());
        for &waiter in &waiters {
            assert!(limiter.poll_acquire(&mut cx, waiter).is_none());
        }

        drop(first);
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(Some(h)) if h == waiters[0]));
        assert!(limiter.poll_head(&mut cx).is_pending());
        assert_eq!(limiter.waiters.len(), waiters.len() - 1);
        assert!(limiter.poll_acquire(&mut cx, newcomer).is_none());

        // An outstanding notification must not reserve more of the node-wide capacity.
        drop(second);
        assert!(limiter.poll_head(&mut cx).is_pending());
        let Poll::Ready(other_partition) = capacity.poll_acquire(&mut cx) else {
            panic!("the other partition must be able to acquire the second permit");
        };
        let permit = limiter.poll_acquire(&mut cx, waiters[0]).unwrap();
        assert!(limiter.poll_head(&mut cx).is_pending());
        drop(permit);

        // Draining a large backlog is linear in the number of waiters, in FIFO order.
        for &waiter in waiters.iter().skip(1).chain(std::iter::once(&newcomer)) {
            assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(Some(h)) if h == waiter));
            assert!(limiter.poll_head(&mut cx).is_pending());
            drop(limiter.poll_acquire(&mut cx, waiter).unwrap());
        }
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(None)));
        drop(other_partition);
        assert_eq!(capacity.available_permits(), 2);
    }

    #[test]
    fn cancelled_waiters_release_capacity_and_wake_the_next_queue() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let [first, second, third, fourth] = std::array::from_fn(|_| handles.insert(()));
        let wake = Arc::new(TestWaker(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);
        let mut capacity = Concurrency::new(Some(NonZeroUsize::MIN));
        let Poll::Ready(held) = capacity.poll_acquire(&mut cx) else {
            panic!("initial capacity is available");
        };
        let mut limiter = InvokerConcurrencyLimiter::new(capacity.clone());
        for waiter in [first, second, third, fourth] {
            assert!(limiter.poll_acquire(&mut cx, waiter).is_none());
        }

        limiter.remove_from_waiters(fourth);
        limiter.remove_from_waiters(first);
        assert!(wake.0.swap(false, Ordering::Relaxed));
        drop(held);
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(Some(h)) if h == second));
        wake.0.store(false, Ordering::Relaxed);

        // The notified queue is no longer in the wait-list or marked blocked by eligibility.
        assert!(limiter.cancel_notification(second));
        assert!(wake.0.swap(false, Ordering::Relaxed));
        let Poll::Ready(other_partition) = capacity.poll_acquire(&mut cx) else {
            panic!("cancelling a notification must release its reserved permit");
        };
        assert!(limiter.poll_head(&mut cx).is_pending());

        // Cancelling the last pending acquisition must not leave a hidden semaphore waiter.
        limiter.remove_from_waiters(third);
        drop(other_partition);
        assert_eq!(capacity.available_permits(), 1);
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(None)));

        // A fresh load after idle can acquire immediately.
        drop(limiter.poll_acquire(&mut cx, fourth).unwrap());
        assert_eq!(capacity.available_permits(), 1);
    }
}
