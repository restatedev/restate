// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Lets work in while a resource is open, and tells the resource's owner when
/// all of that work has finished.
///
/// Before a piece of work uses the resource, it asks the gate for a *lease*
/// ([`Gate::try_acquire`]), and it holds the lease until it is completely
/// done. The gate counts the leases it has handed out. To shut the resource
/// down, the owner does two things:
///
/// 1. **Seal** the gate ([`Gate::seal`]). From then on, the gate refuses new
///    leases.
/// 2. **Drain** the gate ([`Gate::drain`]). This waits until every lease that
///    was handed out earlier has been dropped.
///
/// After that, no work uses the resource anymore, and the owner can release it.
///
/// ```
/// use std::sync::Arc;
///
/// use restate_util_lifecycle::Gate;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let gate = Arc::new(Gate::default());
///
/// // While the resource is open, each piece of work holds a lease.
/// let lease = gate.try_acquire().expect("the gate is open");
/// // ... use the resource ...
/// drop(lease);
///
/// // To shut down, refuse new work, then wait for the work already let in.
/// gate.seal();
/// assert!(gate.try_acquire().is_none());
/// gate.drain().await;
/// // Nothing uses the resource anymore. It is safe to release it.
/// # }
/// ```
///
/// Sealing is permanent. To accept work again, create a new gate.
///
/// # When is a piece of work finished?
///
/// A lease covers the whole life of a piece of work: waiting in a queue,
/// running, and handing back its result. Two rules follow from that:
///
/// - Drop whatever the work holds, such as handles or buffers, *before* the
///   lease. Otherwise `drain` can return while something still holds them.
/// - Cancelling a request, or dropping the future that waits for its result,
///   doesn't finish the work. Work that is queued or running keeps its lease
///   until it is really done.
///
/// # Two kinds of leases
///
/// | Lease          | Use it for                                                       |
/// |----------------|------------------------------------------------------------------|
/// | [`Lease`]      | Work that runs right where it was let in. It borrows the gate.   |
/// | [`OwnedLease`] | Work that moves to another thread. It holds its own `Arc<Gate>`. |
///
/// ```
/// # use std::sync::Arc;
/// # use restate_util_lifecycle::Gate;
/// # let gate = Arc::new(Gate::default());
/// let lease = gate.try_acquire().expect("the gate is open").into_owned();
/// let worker = std::thread::spawn(move || {
///     // ... do the work ...
///     drop(lease); // The work is done.
/// });
/// # worker.join().unwrap();
/// ```
///
/// Sealing only refuses *new* work. Work that already holds a lease can still
/// turn it into an owned lease ([`Lease::into_owned`]), or take an extra lease
/// for follow-up work ([`Lease::fork_owned`], [`OwnedLease::fork`]), after the
/// gate was sealed.
///
/// # Trees of gates
///
/// Resources often live inside other resources, such as tables inside a
/// database. Their gates can form the same tree ([`Gate::child`]):
///
/// ```text
///                database
///              /          \
///         orders          customers
///         /    \               \
///   session 1  session 2     session 3
/// ```
///
/// Shutting down a gate also shuts down everything below it. Sealing
/// `database` seals all six gates, and draining it waits for the leases of all
/// six. Sealing `orders` seals `orders` and its two sessions, and leaves
/// `database` and `customers` running.
#[derive(Debug, Default)]
pub struct Gate {
    state: Mutex<State>,
    drained: Notify,
    // Strong, while a parent only keeps weak links to its children, so the
    // links never form a cycle.
    parent: Option<Arc<Gate>>,
}

#[derive(Debug, Default)]
struct State {
    sealed: bool,
    active: usize,
    // Weak, so a child is freed as soon as its owner drops it.
    children: Vec<Weak<Gate>>,
}

impl Gate {
    /// Creates a gate below this one.
    ///
    /// The child works like any other gate, and can be sealed and drained on
    /// its own. In addition:
    ///
    /// - sealing this gate also seals the child, and
    /// - draining this gate also waits for the child's leases.
    ///
    /// A child that is created after this gate was sealed starts out sealed.
    /// This gate only keeps a weak link to the child, so dropping the child
    /// frees it.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use restate_util_lifecycle::Gate;
    /// let database = Arc::new(Gate::default());
    /// let table = database.child();
    /// assert!(table.is_inside(&database));
    ///
    /// database.seal();
    /// assert!(table.try_acquire().is_none());
    /// ```
    pub fn child(self: &Arc<Self>) -> Arc<Gate> {
        let mut state = self.state.lock();
        let child = Arc::new(Gate {
            state: Mutex::new(State {
                sealed: state.sealed,
                ..State::default()
            }),
            drained: Notify::new(),
            parent: Some(Arc::clone(self)),
        });
        state.children.retain(|child| child.strong_count() > 0);
        state.children.push(Arc::downgrade(&child));
        child
    }

    /// Whether this gate is `other`, or somewhere below it.
    ///
    /// Code that takes a gate from its caller can use this to check that the
    /// gate belongs to the right tree. Otherwise, shutting down that tree
    /// wouldn't wait for the work counted in the gate.
    pub fn is_inside(&self, other: &Gate) -> bool {
        let mut gate = self;
        loop {
            if std::ptr::eq(gate, other) {
                return true;
            }
            match &gate.parent {
                Some(parent) => gate = parent,
                None => return false,
            }
        }
    }

    /// Lets one piece of work in, and returns its lease. Returns `None` once
    /// the gate is sealed.
    ///
    /// The lease borrows the gate. To move the work to another thread, turn
    /// the lease into an [`OwnedLease`] with [`Lease::into_owned`].
    pub fn try_acquire(self: &Arc<Self>) -> Option<Lease<'_>> {
        let mut state = self.state.lock();
        if state.sealed {
            return None;
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("too many active operations");
        Some(Lease { gate: self })
    }

    /// Refuses new work, in this gate and in every gate below it. Leases that
    /// were handed out earlier stay valid.
    ///
    /// Sealing can't be undone.
    pub fn seal(&self) {
        // Seal the children after unlocking, so this never holds two locks at
        // once. A child created in between sees this gate sealed and starts out
        // sealed itself.
        let children = {
            let mut state = self.state.lock();
            state.sealed = true;
            live_children(&state)
        };
        for child in children {
            child.seal();
        }
    }

    /// Waits until every lease of this gate, and of every gate below it, has
    /// been dropped.
    ///
    /// Seal the gate first (checked in debug builds). Sealing is what makes the
    /// wait final: once it returns, no work runs and none can start. To wait
    /// without sealing, use [`Gate::wait_idle`].
    ///
    /// Dropping the returned future stops the wait, and has no other effect.
    pub async fn drain(&self) {
        debug_assert!(self.state.lock().sealed, "seal the gate before draining");
        self.wait_idle().await;
        // A sealed gate that became idle stays idle, because new leases can only
        // be forked from existing ones. So the gates can be waited for one after
        // the other.
        let mut pending = live_children(&self.state.lock());
        while let Some(gate) = pending.pop() {
            gate.wait_idle().await;
            pending.extend(live_children(&gate.state.lock()));
        }
    }

    /// Waits until no lease of this gate is held, without sealing it. Gates
    /// below this one aren't considered.
    ///
    /// New work can start as soon as this returns, so it isn't a shutdown
    /// barrier. It suits gates that are never sealed, such as a gate for
    /// cleanup work that has to keep running during shutdown.
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

    /// Whether no lease of this gate is held right now. Gates below this one
    /// aren't considered. As with [`Gate::wait_idle`], new work can start
    /// right after.
    pub fn is_idle(&self) -> bool {
        self.state.lock().active == 0
    }

    // Adds a lease for work that was already let in, so sealing doesn't block
    // it. The caller's own lease guarantees that the count isn't zero.
    fn fork(self: &Arc<Self>) -> OwnedLease {
        let mut state = self.state.lock();
        debug_assert_ne!(state.active, 0);
        state.active = state
            .active
            .checked_add(1)
            .expect("too many active operations");
        OwnedLease { gate: self.clone() }
    }

    /// Opens a sealed and drained gate again, so tests can reuse a fixture.
    /// Panics if a lease is still held. Any earlier `drain` or `wait_idle` call
    /// must have returned. Gates below this one stay sealed.
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

fn live_children(state: &State) -> Vec<Arc<Gate>> {
    state.children.iter().filter_map(Weak::upgrade).collect()
}

/// Permission for one piece of work to use the gate's resource. The work is
/// finished when the lease is dropped, so drop everything the work holds
/// first.
///
/// This lease borrows its gate, which suits work that runs right where it was
/// let in. To move the work to another thread, turn the lease into an
/// [`OwnedLease`] with [`Lease::into_owned`]. A lease can't be cloned: to
/// start follow-up work, take an extra lease with [`Lease::fork_owned`].
#[derive(Debug)]
#[must_use = "dropping the lease completes the admitted operation"]
pub struct Lease<'a> {
    gate: &'a Arc<Gate>,
}

impl<'a> Lease<'a> {
    /// The gate that handed out this lease.
    pub fn gate(&self) -> &'a Arc<Gate> {
        self.gate
    }

    /// Turns this lease into an [`OwnedLease`], so that the work can move to
    /// another thread.
    ///
    /// It is still the same piece of work, so the gate's count doesn't change,
    /// and this works even after the gate was sealed. For example, a write can
    /// first try to finish right away under this lease and, if it would have to
    /// wait, hand the same lease to a retry on a background thread.
    pub fn into_owned(self) -> OwnedLease {
        let owned = OwnedLease {
            gate: Arc::clone(self.gate),
        };
        // The owned lease takes over; skip our `Drop` so we don't release twice.
        std::mem::forget(self);
        owned
    }

    /// Returns an extra lease for follow-up work, while keeping this one.
    ///
    /// The follow-up work is part of work that was already let in, so this
    /// works even after the gate was sealed. For example, an operation can hand
    /// part of its work to another thread and keep going with the rest.
    pub fn fork_owned(&self) -> OwnedLease {
        self.gate.fork()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

/// Permission for one piece of work to use the gate's resource. Unlike a
/// [`Lease`], it can move to other threads and outlive the code that started
/// the work.
///
/// The lease belongs to the work itself, not to whoever waits for its result:
/// dropping the future that waits for the result doesn't drop the lease.
#[derive(Debug)]
#[must_use = "dropping the lease completes the admitted operation"]
pub struct OwnedLease {
    gate: Arc<Gate>,
}

impl OwnedLease {
    /// Returns an extra lease for follow-up work, while keeping this one. Like
    /// [`Lease::fork_owned`], this works even after the gate was sealed.
    pub fn fork(&self) -> Self {
        self.gate.fork()
    }
}

impl Drop for OwnedLease {
    fn drop(&mut self) {
        self.gate.release();
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
    async fn borrowed_lease_promotes_after_sealing_and_wakes_all_drainers() {
        let gate = Arc::new(Gate::default());
        let inline = gate.try_acquire().unwrap();
        assert_eq!(Arc::strong_count(&gate), 1);
        drop(inline);
        assert_eq!(gate.state.lock().active, 0);

        let first = gate.try_acquire().unwrap();
        let second = gate.try_acquire().unwrap();
        gate.seal();
        assert!(gate.try_acquire().is_none());
        let wake_count = Arc::new(WakeCount::default());
        let waker = Waker::from(wake_count.clone());
        let mut cx = Context::from_waker(&waker);
        let mut cancelled = Box::pin(gate.drain());
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);

        let mut drain_a = pin!(gate.drain());
        let mut drain_b = pin!(gate.drain());
        assert!(drain_a.as_mut().poll(&mut cx).is_pending());
        assert!(drain_b.as_mut().poll(&mut cx).is_pending());
        let owned = first.into_owned();
        assert_eq!(gate.state.lock().active, 2);
        assert_eq!(Arc::strong_count(&gate), 2);
        let continuation = owned.fork();
        assert_eq!(gate.state.lock().active, 3);
        drop(second);
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 0);
        assert!(drain_a.as_mut().poll(&mut cx).is_pending());
        drop(owned);
        assert!(drain_a.as_mut().poll(&mut cx).is_pending());
        drop(continuation);
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 2);
        drain_a.await;
        drain_b.await;
        assert_eq!(Arc::strong_count(&gate), 1);
        assert!(gate.try_acquire().is_none());
    }

    #[tokio::test]
    async fn sealing_and_draining_cover_the_whole_subtree() {
        let root = Arc::new(Gate::default());
        let child = root.child();
        let grandchild = child.child();
        assert!(grandchild.is_inside(&root));
        assert!(root.is_inside(&root));
        assert!(!root.is_inside(&child));

        // A child can be sealed on its own, and its parent only keeps a weak link
        // to it.
        let sealed_alone = root.child();
        sealed_alone.seal();
        assert!(sealed_alone.try_acquire().is_none());
        assert!(root.try_acquire().is_some());
        let weak = Arc::downgrade(&sealed_alone);
        drop(sealed_alone);
        assert!(weak.upgrade().is_none());

        let lease = grandchild.try_acquire().unwrap();
        assert!(Arc::ptr_eq(lease.gate(), &grandchild));
        root.seal();
        assert!(child.try_acquire().is_none());
        assert!(grandchild.try_acquire().is_none());
        assert!(root.child().try_acquire().is_none());

        let mut drain = pin!(root.drain());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(drain.as_mut().poll(&mut cx).is_pending());
        let owned = lease.into_owned();
        assert!(drain.as_mut().poll(&mut cx).is_pending());
        drop(owned);
        drain.await;
    }
}
