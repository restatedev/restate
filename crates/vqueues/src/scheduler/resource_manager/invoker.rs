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

use super::Waiters;
use crate::scheduler::VQueueHandle;

pub struct InvokerConcurrencyLimiter {
    limiter: Concurrency,
    waiters: Waiters,
    cached_permit: Permit,
}

impl InvokerConcurrencyLimiter {
    pub fn new(limiter: Concurrency) -> Self {
        Self {
            limiter,
            waiters: Default::default(),
            cached_permit: Permit::new_empty(),
        }
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.waiters.retain(|h| *h != vqueue);
    }

    fn pop_and_advance(&mut self, cx: &mut std::task::Context<'_>) {
        // pop ourselves.
        self.waiters.pop_front();

        if !self.waiters.is_empty() {
            cx.waker().wake_by_ref();
        }
    }

    pub(super) fn poll_acquire(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
    ) -> Option<Permit> {
        if self.waiters.is_empty() {
            self.waiters.push_back(vqueue);
        }

        if self.waiters.front().is_some_and(|waiter| waiter == &vqueue) {
            tracing::trace!("Checking the head vqueue {vqueue:?} for invoker concurrency permits");
            // cached permit exists, return it.
            if let Some(permit) = self.cached_permit.split(1) {
                self.pop_and_advance(cx);
                return Some(permit);
            }

            if let Poll::Ready(invoker_permit) = self.limiter.poll_acquire(cx) {
                self.pop_and_advance(cx);
                Some(invoker_permit)
            } else {
                // waker registered
                None
            }
        } else {
            // We need to stand behind
            self.waiters.push_back(vqueue);
            None
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
