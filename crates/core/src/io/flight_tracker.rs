// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Counts unfinished work in a scope, so its owner knows when it is safe to
//! shut down.
//!
//! Every operation holds a lease until it is completely finished. To shut down,
//! the owner seals the tracker (no new leases) and drains it (waits for the
//! outstanding ones):
//!
//! ```ignore
//! let flights = Arc::new(FlightTracker::default());
//!
//! // Normal operation
//! let lease = flights.try_acquire().ok_or(AdmissionClosed)?;
//! db.write(batch);
//! drop(lease);
//!
//! // Shutdown
//! flights.seal();
//! flights.drain().await;
//! // No operation is using `db` anymore; safe to close it.
//! ```
//!
//! "Finished" means more than "the caller got its answer". A lease covers the
//! whole life of the work: waiting in a queue, running, and handing back the
//! result. Two rules follow:
//!
//! - Release resources (handles, buffers, snapshots) *before* the lease.
//!   Otherwise `drain` can return while something still holds them.
//! - Cancelling a request or dropping its result future does not finish the
//!   work. Queued or running work keeps its lease until it is actually done.
//!
//! Sealing only stops new admissions. Work that already holds a lease can still
//! move it to another thread ([`Lease::into_owned`]) or split it into more
//! leases ([`Lease::fork_owned`], [`OwnedLease::fork`]) after sealing.

use std::sync::Arc;

use tokio::sync::Notify;

use restate_platform::sync::Mutex;

/// Tracks in-flight work for one scope.
///
/// Sealing is permanent: to accept work again, create a new tracker. The
/// tracker only counts; it owns no resources and needs no registration.
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
    /// Admits one operation and returns its lease, or `None` once sealed.
    ///
    /// The lease borrows the `Arc` instead of cloning it, which keeps inline
    /// operations cheap. Use [`Lease::into_owned`] to move it to another thread.
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

    /// Stops admitting new operations. Leases already handed out stay valid.
    pub fn seal(&self) {
        self.state.lock().sealed = true;
    }

    /// Waits until every outstanding lease is released.
    ///
    /// Call [`Self::seal`] first (debug-asserted); that is what makes this a
    /// final barrier rather than a snapshot like [`Self::wait_idle`]. Dropping
    /// the future is harmless: it affects neither admission nor leases.
    pub async fn drain(&self) {
        debug_assert!(self.state.lock().sealed, "seal admission before draining");
        self.wait_idle().await;
    }

    /// Waits until no lease is outstanding, without sealing.
    ///
    /// This is a point-in-time observation, not a shutdown barrier: new work can
    /// be admitted as soon as it returns. If that matters, the caller must stop
    /// producers itself. Useful for trackers that are never sealed, e.g. cleanup
    /// work that must still be accepted during shutdown.
    pub async fn wait_idle(&self) {
        loop {
            // Create the `Notified` future before checking the count. It receives
            // `notify_waiters` calls from the moment it exists, so a last release
            // landing between the check and the `.await` still wakes us.
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

    /// Whether no lease is outstanding right now. Like [`Self::wait_idle`], only a snapshot.
    pub fn is_idle(&self) -> bool {
        self.state.lock().active == 0
    }

    // Adds a lease for work that was already admitted, so sealing doesn't block
    // it. The caller's own live lease guarantees the count is nonzero.
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

    /// Reopens a drained tracker so test fixtures can be reused. Panics if work
    /// is outstanding. Earlier `drain`/`wait_idle` calls must have returned.
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
        // Waking a waiter can run arbitrary code, so do it after unlocking.
        if drained {
            self.drained.notify_waiters();
        }
    }
}

/// A lease that borrows its tracker, for work running on the caller's stack
/// (inline execution, or the submission path itself).
///
/// Dropping it marks the work as finished, so release the operation's storage
/// references first. It can't be cloned: use [`Self::fork_owned`] to account for
/// extra work, or [`Self::into_owned`] to hand the work to another thread.
#[derive(Debug)]
#[must_use = "dropping the lease completes the admitted operation"]
pub struct Lease<'a> {
    tracker: &'a Arc<FlightTracker>,
}

impl Lease<'_> {
    /// Converts into an [`OwnedLease`] that can move to a background thread.
    ///
    /// Same work, same count: nothing is admitted again, so this also works
    /// after sealing. For example, a write can try a nonblocking attempt inline
    /// under the borrowed lease and, if that would stall, move the same lease
    /// into a background retry.
    pub fn into_owned(self) -> OwnedLease {
        let owned = OwnedLease {
            tracker: Arc::clone(self.tracker),
        };
        // The owned lease takes over; skip our `Drop` so we don't release twice.
        std::mem::forget(self);
        owned
    }

    /// Adds an owned lease while keeping this one, e.g. to hand work to another
    /// thread while the caller still has something left to do.
    ///
    /// The count goes up by one, even after sealing, since this continues work
    /// that was already admitted. [`TrackedExecutor`](super::TrackedExecutor)
    /// uses this to keep the scope busy until its submit call returns, while the
    /// forked lease travels with the queued task.
    pub fn fork_owned(&self) -> OwnedLease {
        self.tracker.fork()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.tracker.release();
    }
}

/// A lease that owns a handle to its tracker, so it can move to other threads
/// and outlive the caller that admitted the work.
///
/// It belongs to the queued or running work, not to whoever awaits the result:
/// dropping a result future does not release it.
#[derive(Debug)]
#[must_use = "dropping the lease completes the admitted operation"]
pub struct OwnedLease {
    tracker: Arc<FlightTracker>,
}

impl OwnedLease {
    /// Adds a lease for follow-up work. Take it before dropping this one, so the
    /// count never touches zero in between.
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
