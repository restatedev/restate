// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Admission and completion accounting, independent of resources and execution.
//!
//! A lease represents unfinished work, including time before dispatch or after
//! execution while a result awaits handoff. Release resources before the lease.
//! Sealing rejects new work; existing leases can transfer or fork responsibility.
//! Neither cancellation nor dropping a result observer completes that work.

use std::sync::Arc;

use tokio::sync::Notify;

use restate_platform::sync::Mutex;

/// One admission lifetime. Sealing is permanent; reopening requires a fresh instance.
/// This state owns no storage resources and requires no manager registration.
#[derive(Debug, Default)]
pub struct FlightTracker {
    state: Mutex<State>,
    drained: Notify,
}

#[derive(Debug, Default)]
struct State {
    sealed: bool,
    active: usize,
}

impl FlightTracker {
    /// Admit an operation without cloning the shared state. Returns `None` after sealing.
    pub fn try_acquire(self: &Arc<Self>) -> Option<Lease<'_>> {
        let mut state = self.state.lock();
        if state.sealed {
            return None;
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("too many active operations");
        Some(Lease { tracker: self })
    }

    /// Reject further operations. Already admitted operations retain their leases.
    pub fn seal(&self) {
        self.state.lock().sealed = true;
    }

    /// Wait for all admitted operations to release their leases.
    /// The lifetime must be sealed before waiting.
    /// Cancelling this wait does not affect admission or outstanding leases.
    pub async fn drain(&self) {
        debug_assert!(self.state.lock().sealed, "seal admission before draining");
        self.wait_idle().await;
    }

    /// Wait for currently outstanding work, without sealing admission.
    ///
    /// The caller must separately control producers. This is an idle observation,
    /// not a final shutdown barrier: new work can be admitted after it returns.
    pub async fn wait_idle(&self) {
        loop {
            // notify_waiters remembers notifications since this future's creation,
            // including a final release between the count check and the await.
            let notified = self.drained.notified();
            {
                let state = self.state.lock();
                if state.active == 0 {
                    return;
                }
            }
            notified.await;
        }
    }

    pub fn is_idle(&self) -> bool {
        self.state.lock().active == 0
    }

    // A live lease keeps the count nonzero, including after sealing. Forking is
    // continuation of accepted work, not a new admission attempt.
    fn fork(self: &Arc<Self>) -> OwnedLease {
        let mut state = self.state.lock();
        debug_assert_ne!(state.active, 0);
        state.active = state
            .active
            .checked_add(1)
            .expect("too many active operations");
        OwnedLease {
            tracker: self.clone(),
        }
    }

    /// Reuse a drained fixture. All previous waiters must have finished.
    #[cfg(any(test, feature = "test-util"))]
    pub fn reset(&self) {
        let mut state = self.state.lock();
        assert_eq!(state.active, 0, "cannot reset outstanding work");
        state.sealed = false;
    }

    fn release(&self) {
        let drained = {
            let mut state = self.state.lock();
            state.active -= 1;
            state.active == 0
        };
        // Waking a waiter can execute user code. Keep it outside the state lock.
        if drained {
            self.drained.notify_waiters();
        }
    }
}

/// A non-cloneable completion guard borrowed by inline execution.
/// Release operation-owned storage references before dropping this guard.
#[derive(Debug)]
#[must_use = "dropping the lease completes the admitted operation"]
pub struct Lease<'a> {
    tracker: &'a Arc<FlightTracker>,
}

impl Lease<'_> {
    /// Transfer the existing admission to background execution. The count never changes,
    /// and promotion remains valid after sealing; it is not a new admission attempt.
    pub fn into_owned(self) -> OwnedLease {
        let owned = OwnedLease {
            tracker: Arc::clone(self.tracker),
        };
        // The owned lease now accounts for exactly this admission.
        std::mem::forget(self);
        owned
    }

    /// Keep this participant alive while transferring another to detached work.
    pub fn fork_owned(&self) -> OwnedLease {
        self.tracker.fork()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.tracker.release();
    }
}

/// A completion guard that can outlive the admitting caller.
/// It belongs to the queued/running operation, not its result waiter.
#[derive(Debug)]
#[must_use = "dropping the lease completes the admitted operation"]
pub struct OwnedLease {
    tracker: Arc<FlightTracker>,
}

impl OwnedLease {
    /// Account for a continuation before releasing the current participant.
    pub fn fork(&self) -> Self {
        self.tracker.fork()
    }
}

impl Drop for OwnedLease {
    fn drop(&mut self) {
        self.tracker.release();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    use super::*;

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn borrowed_admission_promotes_after_sealing_and_wakes_all_drainers() {
        let admission = Arc::new(FlightTracker::default());
        let inline = admission.try_acquire().unwrap();
        assert_eq!(Arc::strong_count(&admission), 1);
        drop(inline);
        assert_eq!(admission.state.lock().active, 0);

        let first = admission.try_acquire().unwrap();
        let second = admission.try_acquire().unwrap();
        admission.seal();
        assert!(admission.try_acquire().is_none());
        let wake_count = Arc::new(WakeCount::default());
        let waker = Waker::from(wake_count.clone());
        let mut cx = Context::from_waker(&waker);
        let mut cancelled = Box::pin(admission.drain());
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);

        let mut drain_a = pin!(admission.drain());
        let mut drain_b = pin!(admission.drain());
        assert!(drain_a.as_mut().poll(&mut cx).is_pending());
        assert!(drain_b.as_mut().poll(&mut cx).is_pending());
        let owned = first.into_owned();
        assert_eq!(admission.state.lock().active, 2);
        assert_eq!(Arc::strong_count(&admission), 2);
        let continuation = owned.fork();
        assert_eq!(admission.state.lock().active, 3);
        drop(second);
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 0);
        assert!(drain_a.as_mut().poll(&mut cx).is_pending());
        drop(owned);
        assert!(drain_a.as_mut().poll(&mut cx).is_pending());
        drop(continuation);
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 2);
        drain_a.await;
        drain_b.await;
        assert_eq!(Arc::strong_count(&admission), 1);
        assert!(admission.try_acquire().is_none());
    }
}
