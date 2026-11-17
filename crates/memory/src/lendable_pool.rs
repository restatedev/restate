// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lending a [`LocalMemoryPool`] to another task, for example a storage worker
//! reading on behalf of an invocation.

use std::ops::{Deref, DerefMut};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future;
use tokio::sync::{Notify, oneshot};

use crate::{LocalMemoryLease, LocalMemoryPool, OutOfMemory, ReserveMemory};

/// A [`LocalMemoryPool`] that can be lent to another task.
///
/// The pool is only reachable by [claiming](Self::claim) it, which waits for a
/// lent pool to come back.
pub struct LendableMemoryPool {
    state: State,
}

enum State {
    Here(LocalMemoryPool),
    Lent(oneshot::Receiver<LocalMemoryPool>),
}

impl LendableMemoryPool {
    pub fn new(pool: LocalMemoryPool) -> Self {
        Self {
            state: State::Here(pool),
        }
    }

    /// Claims the pool, waiting for it to come back if it's lent.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: if the pool hasn't come back yet, it stays lent.
    pub async fn claim(&mut self) -> ClaimedMemoryPool<'_> {
        if let State::Lent(returning) = &mut self.state {
            let pool = returned(returning).await;
            self.state = State::Here(pool);
        }
        ClaimedMemoryPool {
            state: &mut self.state,
        }
    }

    /// Returns the pool, waiting for it to come back if it's lent.
    pub async fn into_inner(self) -> LocalMemoryPool {
        match self.state {
            State::Here(pool) => pool,
            State::Lent(mut returning) => returned(&mut returning).await,
        }
    }
}

async fn returned(returning: &mut oneshot::Receiver<LocalMemoryPool>) -> LocalMemoryPool {
    returning
        .await
        .expect("a LentMemoryPool sends the pool back when dropped")
}

/// A [`LendableMemoryPool`] whose pool is here, see [`LendableMemoryPool::claim`].
///
/// Dereferences to the pool. The pool can't be lent by anyone else while this
/// exists, so [lending](Self::lend) it doesn't wait.
pub struct ClaimedMemoryPool<'a> {
    /// Always [`State::Here`].
    state: &'a mut State,
}

impl<'a> ClaimedMemoryPool<'a> {
    /// The pool, borrowed for as long as it was claimed.
    pub fn into_mut(self) -> &'a mut LocalMemoryPool {
        match self.state {
            State::Here(pool) => pool,
            State::Lent(_) => unreachable!("a claimed pool is here"),
        }
    }

    /// Lends the pool. It comes back when the returned [`LentMemoryPool`] is
    /// dropped, for example when the task holding it finishes or is cancelled.
    ///
    /// The [`PinHandle`] is for whoever receives the leases the borrower
    /// reserves.
    pub fn lend(self) -> (LentMemoryPool, PinHandle) {
        let (back, returning) = oneshot::channel();
        let State::Here(pool) = std::mem::replace(self.state, State::Lent(returning)) else {
            unreachable!("a claimed pool is here")
        };
        let pinned = Arc::<Pinned>::default();
        let lent = LentMemoryPool {
            inner: Some((pool, back)),
            pinned: Arc::clone(&pinned),
        };
        (lent, PinHandle(pinned))
    }
}

impl Deref for ClaimedMemoryPool<'_> {
    type Target = LocalMemoryPool;

    fn deref(&self) -> &LocalMemoryPool {
        match &*self.state {
            State::Here(pool) => pool,
            State::Lent(_) => unreachable!("a claimed pool is here"),
        }
    }
}

impl DerefMut for ClaimedMemoryPool<'_> {
    fn deref_mut(&mut self) -> &mut LocalMemoryPool {
        ClaimedMemoryPool {
            state: &mut *self.state,
        }
        .into_mut()
    }
}

/// A [`LocalMemoryPool`] lent with [`ClaimedMemoryPool::lend`]. Sends the pool
/// back when dropped.
///
/// If the [`LendableMemoryPool`] is gone by then, the pool is dropped here.
pub struct LentMemoryPool {
    /// `None` only while dropping.
    inner: Option<(LocalMemoryPool, oneshot::Sender<LocalMemoryPool>)>,
    pinned: Arc<Pinned>,
}

impl LentMemoryPool {
    /// See [`LocalMemoryPool::try_reserve`].
    pub fn try_reserve(&mut self, size: usize) -> Option<LocalMemoryLease> {
        self.pool_mut().try_reserve(size)
    }

    /// Like [`LocalMemoryPool::reserve`], where the pinned memory is what the
    /// [`PinHandle`] reports. Checks again whenever more is pinned.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: no memory is reserved until this returns.
    pub async fn reserve(&mut self, size: usize) -> Result<LocalMemoryLease, OutOfMemory> {
        let pinned = Arc::clone(&self.pinned);
        let pool = self.pool_mut();
        loop {
            // Created before checking, so that changes in between wake us.
            let available = pin!(pool.availability_notified());
            let more_pinned = pin!(pinned.more_pinned.notified());

            if let Some(lease) = pool.try_reserve(size) {
                return Ok(lease);
            }
            pool.check_out_of_memory(size, pinned.amount.load(Ordering::Relaxed))?;

            future::select(available, more_pinned).await;
        }
    }

    fn pool_mut(&mut self) -> &mut LocalMemoryPool {
        &mut self.inner.as_mut().expect("not dropped").0
    }
}

impl ReserveMemory for LentMemoryPool {
    fn try_reserve(&mut self, size: usize) -> Option<LocalMemoryLease> {
        LentMemoryPool::try_reserve(self, size)
    }

    fn reserve(
        &mut self,
        size: usize,
    ) -> impl Future<Output = Result<LocalMemoryLease, OutOfMemory>> + Send {
        LentMemoryPool::reserve(self, size)
    }
}

/// Memory reserved from a [`LentMemoryPool`] that is held until its holder is
/// done. Waiting for it to be released would never end.
#[derive(Default)]
struct Pinned {
    amount: AtomicUsize,
    /// Wakes the borrower when more is pinned, to check again if it's out of
    /// memory.
    more_pinned: Notify,
}

/// Tells the borrower of a [`LentMemoryPool`] which of the memory it reserved
/// won't be released while it waits, see
/// [`PinnableMemoryStream`](crate::PinnableMemoryStream).
pub struct PinHandle(Arc<Pinned>);

impl PinHandle {
    /// See [`PinnableMemoryStream::pin_memory`](crate::PinnableMemoryStream::pin_memory).
    pub fn pin(&self, amount: usize) {
        self.0.amount.fetch_add(amount, Ordering::Relaxed);
        self.0.more_pinned.notify_waiters();
    }

    /// See [`PinnableMemoryStream::unpin_memory`](crate::PinnableMemoryStream::unpin_memory).
    pub fn unpin(&self, amount: usize) {
        self.0
            .amount
            .update(Ordering::Relaxed, Ordering::Relaxed, |pinned| {
                pinned.saturating_sub(amount)
            });
    }
}

impl Drop for LentMemoryPool {
    fn drop(&mut self) {
        if let Some((pool, back)) = self.inner.take() {
            let _ = back.send(pool);
        }
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LendableMemoryPool>();
    assert_send_sync::<ClaimedMemoryPool<'_>>();
    assert_send_sync::<LentMemoryPool>();
    assert_send_sync::<PinHandle>();
};
