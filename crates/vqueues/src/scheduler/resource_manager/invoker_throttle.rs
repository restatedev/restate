// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::task::Poll;
use std::time::Duration;

use hashbrown::HashMap;
use tokio_util::time::{DelayQueue, delay_queue};

use restate_types::time::MillisSinceEpoch;

use super::Waiters;
use super::permit::ThrottlingToken;
use crate::GlobalTokenBucket;
use crate::scheduler::VQueueHandle;
use crate::scheduler::clock::SchedulerClock;

// Why we are doing this? If we allowed all vqueues from a single partition
// to borrow from the token bucket, they would form a strand of lined-up waiters (in terms
// of time delays, which is global to the node) and that would mean that we'd push vqueues
// of other partitions on the same machine far in the future (after pre-booked strand of this
// partition completes). This would mean that we'd cause unfairness between partitions and how
// they share the global invoker throttling token bucket. To address this, we decide on a
// somewhat arbitrary upper bound on the number of vqueues that can borrow from this partition
// and leave the rest waiting without pre-booking. The downside is that for those vqueues outside
// this bound, we'll not be able to determine their retry_at estimate (unless we add some kind
// of heuristic, if needed).
const MAX_RESERVED_WAITERS: usize = 17;
const ONE_TOKEN: NonZeroU32 = NonZeroU32::MIN;

pub(super) enum ThrottlingAcquire {
    Acquired(ThrottlingToken),
    Blocked {
        estimated_retry_at: Option<MillisSinceEpoch>,
    },
}

struct Reservation {
    estimated_retry_at: MillisSinceEpoch,
    timer_key: Option<delay_queue::Key>,
}

impl Reservation {
    fn is_ready(&self) -> bool {
        self.timer_key.is_none()
    }
}

pub struct InvokerThrottlingLimiter {
    // The global throttling token bucket (shared with other resource managers on this node).
    token_bucket: Option<GlobalTokenBucket>,
    // vqueues currently blocked on invoker throttling.
    waiters: Waiters,
    // Reserved throttling tokens for the first N waiters.
    reservations: HashMap<VQueueHandle, Reservation>,
    delayed_reservations: DelayQueue<VQueueHandle>,
    reservation_window: usize,
    // Avoid repeatedly waking the same ready head before it acquires its reservation.
    notified_head: Option<VQueueHandle>,
}

impl InvokerThrottlingLimiter {
    pub fn new(token_bucket: Option<GlobalTokenBucket>) -> Self {
        let reservation_window = token_bucket.as_ref().map_or(0, |bucket| {
            MAX_RESERVED_WAITERS.min(bucket.limit().burst().get() as usize)
        });

        Self {
            token_bucket,
            waiters: Default::default(),
            reservations: HashMap::with_capacity(reservation_window),
            delayed_reservations: DelayQueue::with_capacity(reservation_window),
            reservation_window,
            notified_head: None,
        }
    }

    pub fn poll_acquire(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
    ) -> ThrottlingAcquire {
        if self.token_bucket.is_none() {
            return ThrottlingAcquire::Acquired(ThrottlingToken);
        }

        if self.waiters.front().is_none_or(|head| *head != vqueue) {
            self.enqueue_waiter(vqueue);
        }
        self.poll_timers(cx);
        self.replenish_reservations();

        if self.waiters.front().is_some_and(|head| *head == vqueue)
            && self.is_reserved_ready(vqueue)
        {
            self.consume_front_reservation(cx, vqueue);
            return ThrottlingAcquire::Acquired(ThrottlingToken);
        }

        ThrottlingAcquire::Blocked {
            estimated_retry_at: self.estimate_for_waiter(vqueue),
        }
    }

    pub fn poll_head(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<VQueueHandle>> {
        if self.token_bucket.is_none() {
            return Poll::Ready(None);
        }

        self.poll_timers(cx);
        self.replenish_reservations();

        let Some(vqueue) = self.waiters.front().copied() else {
            self.notified_head = None;
            return Poll::Ready(None);
        };

        if self
            .notified_head
            .is_some_and(|notified| notified != vqueue)
        {
            self.notified_head = None;
        }

        if !self.is_reserved_ready(vqueue) {
            return Poll::Pending;
        }

        if self
            .notified_head
            .is_some_and(|notified| notified == vqueue)
        {
            return Poll::Pending;
        }

        self.notified_head = Some(vqueue);
        Poll::Ready(Some(vqueue))
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.waiters.retain(|h| *h != vqueue);
        self.notified_head = self.notified_head.filter(|head| *head != vqueue);
        self.remove_reservation(vqueue, true);

        if let Some(head) = self.waiters.front().copied()
            && self.notified_head.is_some_and(|notified| notified != head)
        {
            self.notified_head = None;
        }

        if self.waiters.is_empty() {
            self.notified_head = None;
        }
    }

    fn enqueue_waiter(&mut self, vqueue: VQueueHandle) {
        if !self.waiters.contains(&vqueue) {
            self.waiters.push_back(vqueue);
        }
    }

    fn is_reserved_ready(&self, vqueue: VQueueHandle) -> bool {
        self.reservations
            .get(&vqueue)
            .is_some_and(Reservation::is_ready)
    }

    fn estimate_for_waiter(&self, vqueue: VQueueHandle) -> Option<MillisSinceEpoch> {
        self.reservations
            .get(&vqueue)
            .map(|reservation| reservation.estimated_retry_at)
    }

    fn consume_front_reservation(&mut self, cx: &mut std::task::Context<'_>, vqueue: VQueueHandle) {
        if self.waiters.front().is_some_and(|head| *head == vqueue) {
            self.waiters.pop_front();
        } else {
            self.waiters.retain(|h| *h != vqueue);
        }

        self.notified_head = None;
        self.remove_reservation(vqueue, false);
        self.replenish_reservations();

        if !self.waiters.is_empty() {
            cx.waker().wake_by_ref();
        }
    }

    fn remove_reservation(&mut self, vqueue: VQueueHandle, refund: bool) {
        let Some(mut reservation) = self.reservations.remove(&vqueue) else {
            return;
        };

        if let Some(timer_key) = reservation.timer_key.take() {
            self.delayed_reservations.remove(&timer_key);
        }

        if refund && let Some(token_bucket) = self.token_bucket.as_ref() {
            token_bucket.add_tokens(1.0);
        }
    }

    fn poll_timers(&mut self, cx: &mut std::task::Context<'_>) {
        while let Poll::Ready(Some(expired)) = self.delayed_reservations.poll_expired(cx) {
            let timer_key = expired.key();
            let vqueue = expired.into_inner();

            if let Some(reservation) = self.reservations.get_mut(&vqueue)
                && reservation
                    .timer_key
                    .as_ref()
                    .is_some_and(|queued_key| *queued_key == timer_key)
            {
                reservation.timer_key = None;
            }
        }
    }

    fn replenish_reservations(&mut self) {
        let Some(token_bucket) = self.token_bucket.as_ref() else {
            return;
        };

        let waiters_in_window: Vec<_> = self
            .waiters
            .iter()
            .copied()
            .take(self.reservation_window)
            .collect();

        for vqueue in waiters_in_window {
            if self.reservations.contains_key(&vqueue) {
                continue;
            }

            let retry_after = token_bucket
                .consume_with_borrow(ONE_TOKEN)
                .expect("consuming one token with borrow must fit burst");

            let now = SchedulerClock.now_millis();
            let reservation = if let Some(retry_after) = retry_after {
                let retry_after: Duration = retry_after.into();
                let timer_key = self.delayed_reservations.insert(vqueue, retry_after);
                Reservation {
                    estimated_retry_at: now + retry_after,
                    timer_key: Some(timer_key),
                }
            } else {
                Reservation {
                    estimated_retry_at: now,
                    timer_key: None,
                }
            };

            self.reservations.insert(vqueue, reservation);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::task::Poll;
    use std::time::Duration;

    use slotmap::SlotMap;

    use super::*;
    use crate::scheduler::VQueueHandle;

    fn approx_eq(left: f64, right: f64) {
        assert!((left - right).abs() < 1e-6, "left={left} right={right}");
    }

    #[tokio::test(start_paused = true)]
    async fn head_waiter_gets_retry_estimate_and_acquires_without_reconsuming() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let vq1 = handles.insert(());
        let vq2 = handles.insert(());

        let bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
            ),
            gardal::TokioClock,
        );

        let mut limiter = InvokerThrottlingLimiter::new(Some(bucket.clone()));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        assert!(matches!(
            limiter.poll_acquire(&mut cx, vq1),
            ThrottlingAcquire::Acquired(_)
        ));

        let estimated_retry_at = match limiter.poll_acquire(&mut cx, vq2) {
            ThrottlingAcquire::Blocked { estimated_retry_at } => estimated_retry_at,
            ThrottlingAcquire::Acquired(_) => panic!("expected throttling to block"),
        };
        assert!(estimated_retry_at.is_some());

        assert!(matches!(limiter.poll_head(&mut cx), Poll::Pending));

        tokio::time::advance(Duration::from_secs(1)).await;

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(Some(h)) if h == vq2));
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Pending));
        assert!(matches!(
            limiter.poll_acquire(&mut cx, vq2),
            ThrottlingAcquire::Acquired(_)
        ));

        approx_eq(bucket.balance(), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn only_waiters_inside_reservation_window_get_estimates() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let consume_1 = handles.insert(());
        let consume_2 = handles.insert(());
        let vq1 = handles.insert(());
        let vq2 = handles.insert(());
        let vq3 = handles.insert(());

        let bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(2).unwrap(),
            ),
            gardal::TokioClock,
        );

        let mut limiter = InvokerThrottlingLimiter::new(Some(bucket));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        assert!(matches!(
            limiter.poll_acquire(&mut cx, consume_1),
            ThrottlingAcquire::Acquired(_)
        ));
        assert!(matches!(
            limiter.poll_acquire(&mut cx, consume_2),
            ThrottlingAcquire::Acquired(_)
        ));

        let estimate_1 = match limiter.poll_acquire(&mut cx, vq1) {
            ThrottlingAcquire::Blocked { estimated_retry_at } => estimated_retry_at,
            ThrottlingAcquire::Acquired(_) => panic!("expected throttling to block"),
        };
        let estimate_2 = match limiter.poll_acquire(&mut cx, vq2) {
            ThrottlingAcquire::Blocked { estimated_retry_at } => estimated_retry_at,
            ThrottlingAcquire::Acquired(_) => panic!("expected throttling to block"),
        };
        let estimate_3 = match limiter.poll_acquire(&mut cx, vq3) {
            ThrottlingAcquire::Blocked { estimated_retry_at } => estimated_retry_at,
            ThrottlingAcquire::Acquired(_) => panic!("expected throttling to block"),
        };

        assert!(estimate_1.is_some());
        assert!(estimate_2.is_some());
        assert!(estimate_3.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn removing_waiter_refunds_reserved_token() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let consume = handles.insert(());
        let waiter = handles.insert(());

        let bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
            ),
            gardal::TokioClock,
        );

        let mut limiter = InvokerThrottlingLimiter::new(Some(bucket.clone()));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        assert!(matches!(
            limiter.poll_acquire(&mut cx, consume),
            ThrottlingAcquire::Acquired(_)
        ));

        let estimate = match limiter.poll_acquire(&mut cx, waiter) {
            ThrottlingAcquire::Blocked { estimated_retry_at } => estimated_retry_at,
            ThrottlingAcquire::Acquired(_) => panic!("expected throttling to block"),
        };
        assert!(estimate.is_some());
        assert!(bucket.balance() < 0.0);

        limiter.remove_from_waiters(waiter);

        approx_eq(bucket.balance(), 0.0);
        assert!(matches!(limiter.poll_head(&mut cx), Poll::Ready(None)));
    }
}
