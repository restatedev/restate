// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, ready},
};

use futures::FutureExt;
use tokio::sync::{Notify, futures::OwnedNotified};

/// Shared state for the concurrency limiter.
#[derive(Debug)]
struct ConcurrencyInner {
    size: AtomicUsize,
    inflight: AtomicUsize,
    capacity: Arc<Notify>,
    reclaim: Arc<Notify>,
}

/// An async concurrency limiter that controls how many tasks can run simultaneously.
///
/// Permits are acquired asynchronously and released automatically when dropped.
/// The concurrency limit can be dynamically resized at runtime.
#[derive(Clone, Debug)]
pub struct Concurrency {
    inner: Arc<ConcurrencyInner>,
}

impl Concurrency {
    /// Creates a new concurrency limiter with the given maximum number of permits.
    pub fn new(size: usize) -> Self {
        Self {
            inner: Arc::new(ConcurrencyInner {
                size: AtomicUsize::new(size),
                inflight: AtomicUsize::new(0),
                capacity: Arc::new(Notify::new()),
                reclaim: Arc::new(Notify::new()),
            }),
        }
    }

    /// Returns a future that resolves to a [`Permit`] once capacity is available.
    pub fn acquire(&self) -> PermitFuture {
        PermitFuture::new(self.clone())
    }

    /// Dynamically changes the concurrency limit. If the new limit is larger than
    /// the previous one, waiting tasks are notified so they can attempt to acquire.
    /// If the new limit is smaller, active permit holders are signalled via
    /// [`Permit::poll_reclaimed`] so they can voluntarily release their permits.
    pub fn resize(&self, size: usize) {
        let previous = self.inner.size.swap(size, Ordering::Relaxed);
        match previous.cmp(&size) {
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Less => {
                // capacity increase
                self.inner.capacity.notify_waiters();
            }
            std::cmp::Ordering::Greater => {
                // capacity decrease
                self.inner.reclaim.notify_waiters();
            }
        }
    }

    /// Returns the current concurrency limit.
    pub fn size(&self) -> usize {
        self.inner.size.load(Ordering::Relaxed)
    }

    /// Returns the number of currently acquired (in-flight) permits.
    pub fn acquired(&self) -> usize {
        self.inner.inflight.load(Ordering::Relaxed)
    }

    /// Returns an approximate number of available permits.
    ///
    /// This is a best-effort snapshot since `size` and `inflight` are read
    /// as two separate atomic loads and may be inconsistent under contention.
    pub fn available(&self) -> usize {
        self.size().saturating_sub(self.acquired())
    }
}

/// A guard representing an acquired concurrency permit.
/// The permit is released automatically when dropped, waking one waiting task.
#[derive(Debug)]
pub struct Permit {
    concurrency: Concurrency,
    reclaimed: Reclaimed,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let prev = self
            .concurrency
            .inner
            .inflight
            .fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev != 0);
        self.concurrency.inner.capacity.notify_one();
    }
}

impl Permit {
    /// Polls whether the concurrency limit has been reduced and this permit
    /// should be returned. Returns [`Poll::Ready`] when a reclaim has been
    /// requested (via [`Concurrency::resize`] to a smaller limit).
    ///
    /// Dropping the permit is voluntary — callers should only do so when
    /// they can safely give up the resource (e.g. the resource is idle or
    /// the caller is not waiting on additional work).
    pub fn poll_reclaimed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.reclaimed).poll(cx)
    }
}

#[pin_project::pin_project(project=PermitFutureStateProject)]
enum PermitFutureState {
    TryAcquire,
    Waiting {
        #[pin]
        notified: OwnedNotified,
    },
}

/// A future that resolves to a [`Permit`] when concurrency capacity becomes available.
#[pin_project::pin_project]
pub struct PermitFuture {
    concurrency: Concurrency,
    #[pin]
    state: PermitFutureState,
}

impl PermitFuture {
    fn new(concurrency: Concurrency) -> Self {
        Self {
            concurrency,
            state: PermitFutureState::TryAcquire,
        }
    }
}

impl Future for PermitFuture {
    type Output = Permit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.state.as_mut().project() {
                PermitFutureStateProject::TryAcquire => {
                    // make sure we register interest in waking up
                    this.state.set(PermitFutureState::Waiting {
                        notified: this.concurrency.inner.capacity.clone().notified_owned(),
                    });

                    let PermitFutureStateProject::Waiting { notified } =
                        this.state.as_mut().project()
                    else {
                        unreachable!()
                    };

                    notified.enable();

                    // Unlike `notified` above, `reclaimed` does not need to be enabled before
                    // the notification fires. `resize()` uses `notify_waiters()` which delivers
                    // to every waiter created before the call, regardless of whether it has been
                    // enabled or polled yet.
                    let reclaimed = this.concurrency.inner.reclaim.clone().notified_owned();

                    let mut size = this.concurrency.inner.size.load(Ordering::Relaxed);
                    let mut inflight = this.concurrency.inner.inflight.load(Ordering::Relaxed);

                    while inflight < size {
                        match this.concurrency.inner.inflight.compare_exchange(
                            inflight,
                            inflight + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                return Poll::Ready(Permit {
                                    concurrency: this.concurrency.clone(),
                                    reclaimed: Reclaimed::new(reclaimed),
                                });
                            }
                            Err(value) => {
                                inflight = value;
                                // also update the size in case the size has changed
                                // to make sure we never go above the limits
                                size = this.concurrency.inner.size.load(Ordering::Relaxed);
                            }
                        }
                    }
                }
                PermitFutureStateProject::Waiting { notified } => {
                    ready!(notified.poll(cx));
                    this.state.set(PermitFutureState::TryAcquire);
                }
            }
        }
    }
}

/// A boxed wrapper around [`OwnedNotified`] that makes [`Permit`] `Unpin`.
///
/// [`ResponseFuture`](super::ResponseFuture) polls [`Permit::poll_reclaimed`] while waiting
/// for H2 readiness, then `.take()`s the permit out of its `Option` and moves it into
/// [`PermittedRecvStream`](super::PermittedRecvStream).
///
/// That move-after-poll is only possible if `Permit` is `Unpin`. Since [`OwnedNotified`] is
/// `!Unpin`, we box it here so the `!Unpin` bound does not propagate to `Permit`.
#[derive(Debug)]
struct Reclaimed(Pin<Box<OwnedNotified>>);

impl Reclaimed {
    fn new(notified: OwnedNotified) -> Self {
        Self(Box::pin(notified))
    }
}

impl Future for Reclaimed {
    type Output = <OwnedNotified as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn acquire_within_limit() {
        let conc = Concurrency::new(2);
        let _p1 = conc.acquire().await;
        let _p2 = conc.acquire().await;
        assert_eq!(conc.acquired(), 2);
        assert_eq!(conc.available(), 0);
    }

    #[tokio::test]
    async fn drop_permit_releases_capacity() {
        let conc = Concurrency::new(1);

        let p1 = conc.acquire().await;
        assert_eq!(conc.acquired(), 1);

        // Spawn a task that waits for a permit
        let conc2 = conc.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _p2 = conc2.acquire().await;
            tx.send(conc2.acquired()).unwrap();
        });

        // Give the spawned task a chance to register as a waiter
        tokio::task::yield_now().await;

        // Dropping p1 should wake the waiting task and grant it a permit
        drop(p1);
        let acquired = rx.await.unwrap();
        assert_eq!(acquired, 1);
    }

    #[tokio::test]
    async fn resize_up_unblocks_waiters() {
        let conc = Concurrency::new(0);

        // Spawn tasks that wait for permits on a zero-sized limiter
        let conc1 = conc.clone();
        let (tx1, rx1) = oneshot::channel();
        tokio::spawn(async move {
            let _p = conc1.acquire().await;
            tx1.send(()).unwrap();
        });

        let conc2 = conc.clone();
        let (tx2, rx2) = oneshot::channel();
        tokio::spawn(async move {
            let _p = conc2.acquire().await;
            tx2.send(()).unwrap();
        });

        // Let both tasks reach the waiting state
        tokio::task::yield_now().await;
        assert_eq!(conc.acquired(), 0);

        // Resize to 2 — both waiters should be unblocked
        conc.resize(2);

        rx1.await.unwrap();
        rx2.await.unwrap();
        assert_eq!(conc.size(), 2);
    }

    #[tokio::test]
    async fn resize_down_drains_existing_permits() {
        let conc = Concurrency::new(3);

        let p1 = conc.acquire().await;
        let p2 = conc.acquire().await;
        let p3 = conc.acquire().await;
        assert_eq!(conc.acquired(), 3);

        // Resize down to 1 — existing permits are not revoked
        conc.resize(1);
        assert_eq!(conc.acquired(), 3);

        // New acquires should block until enough permits are dropped
        let conc2 = conc.clone();
        let (tx, mut rx) = oneshot::channel();
        tokio::spawn(async move {
            let _p = conc2.acquire().await;
            tx.send(conc2.acquired()).unwrap();
        });

        tokio::task::yield_now().await;

        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty),
        ));

        // Drop two permits — inflight goes from 3 to 1, which is at the limit.
        // The waiter still can't acquire since 1 + 1 > 1.
        drop(p1);
        drop(p2);

        // we still can't acquire
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty),
        ));

        // Now inflight is 1. Drop p3 (held by the outer scope) would unblock.
        drop(p3);

        let acquired = rx.await.unwrap();
        assert_eq!(acquired, 1);
    }

    #[tokio::test]
    async fn multiple_waiters_released_one_at_a_time() {
        let conc = Concurrency::new(1);
        let p1 = conc.acquire().await;

        // Each spawned task sends its permit back so it stays alive
        let conc2 = conc.clone();
        let (tx1, rx1) = oneshot::channel();
        tokio::spawn(async move {
            let p = conc2.acquire().await;
            tx1.send(p).unwrap();
        });

        let conc3 = conc.clone();
        let (tx2, rx2) = oneshot::channel();
        tokio::spawn(async move {
            let p = conc3.acquire().await;
            tx2.send(p).unwrap();
        });

        tokio::task::yield_now().await;

        // Drop p1 — only one waiter should be woken (notify_one)
        drop(p1);

        // One of the two should complete
        let _permit = tokio::select! {
            p = rx1 => p.unwrap(),
            p = rx2 => p.unwrap(),
        };

        // The other is still waiting since limit is 1
        assert_eq!(conc.acquired(), 1);
    }

    #[tokio::test]
    async fn resize_down_signals_reclaim() {
        let conc = Concurrency::new(2);
        let mut p1 = conc.acquire().await;
        let mut p2 = conc.acquire().await;

        // No reclaim yet — poll_reclaimed should return Pending
        assert!(
            poll_fn(|cx| Poll::Ready(p1.poll_reclaimed(cx)))
                .await
                .is_pending()
        );

        // Shrink the limit — both permit holders should be notified
        conc.resize(1);

        poll_fn(|cx| p1.poll_reclaimed(cx)).await;
        poll_fn(|cx| p2.poll_reclaimed(cx)).await;

        // Permits are still held (reclaim is advisory), inflight unchanged
        assert_eq!(conc.acquired(), 2);

        // Voluntarily drop one permit to honour the new limit
        drop(p1);
        assert_eq!(conc.acquired(), 1);
    }

    #[tokio::test]
    async fn reclaim_not_signalled_on_resize_up() {
        let conc = Concurrency::new(1);
        let mut p1 = conc.acquire().await;

        // Resize up — should NOT trigger reclaim
        conc.resize(3);

        assert!(
            poll_fn(|cx| Poll::Ready(p1.poll_reclaimed(cx)))
                .await
                .is_pending()
        );

        drop(p1);
    }

    #[tokio::test]
    async fn reclaim_then_reacquire() {
        let conc = Concurrency::new(2);
        let mut p1 = conc.acquire().await;
        let p2 = conc.acquire().await;
        assert_eq!(conc.acquired(), 2);

        // Shrink to 1 and drop the reclaimed permit
        conc.resize(1);
        poll_fn(|cx| p1.poll_reclaimed(cx)).await;
        drop(p1);
        assert_eq!(conc.acquired(), 1);

        // Drop p2 so a new acquire succeeds under the reduced limit
        drop(p2);
        let _p3 = conc.acquire().await;
        assert_eq!(conc.acquired(), 1);
    }

    #[tokio::test]
    async fn resize_up_while_at_capacity() {
        let conc = Concurrency::new(1);
        let _p1 = conc.acquire().await;

        // Waiter blocked because at capacity — send permit back to keep it alive
        let conc2 = conc.clone();
        let (tx, mut rx) = oneshot::channel();
        tokio::spawn(async move {
            let p = conc2.acquire().await;
            tx.send(p).unwrap();
        });

        tokio::task::yield_now().await;
        assert_eq!(conc.acquired(), 1);

        // we still can't acquire
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty),
        ));

        // Resize from 1 to 2 — the waiter should now be granted a permit
        conc.resize(2);

        let _p2 = rx.await.unwrap();
        assert_eq!(conc.acquired(), 2);
    }
}
