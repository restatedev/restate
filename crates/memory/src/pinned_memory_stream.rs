// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use pin_project_lite::pin_project;

/// A [`Stream`] that tracks **pinned** (non-reclaimable) memory.
///
/// Consumers that accumulate leases from a budgeted stream (e.g. merging
/// per-item leases into a single aggregate) create a livelock risk: the
/// [`LocalMemoryPool`](crate::LocalMemoryPool) considers all in-flight memory
/// as reclaimable, but accumulated leases won't be released until the entire
/// operation completes.
///
/// By calling [`pin_memory`](Self::pin_memory) after each merge and
/// [`unpin_memory`](Self::unpin_memory) after each split/release, the consumer
/// informs the stream how much of the in-flight memory is pinned. The stream
/// passes this to [`LocalMemoryPool::is_out_of_memory`](crate::LocalMemoryPool::is_out_of_memory)
/// so that truly infeasible requests are detected promptly instead of waiting
/// forever.
pub trait PinnableMemoryStream: Stream {
    /// Inform the stream that `amount` additional bytes of previously yielded
    /// lease data have been pinned (accumulated) by the caller and will **not**
    /// be released until the caller's operation completes.
    fn pin_memory(self: Pin<&mut Self>, amount: usize);

    /// Inform the stream that `amount` bytes of previously pinned data have
    /// been released or consumed, making them reclaimable again.
    fn unpin_memory(self: Pin<&mut Self>, amount: usize);
}

// Blanket impls so that `Pin<&mut T>` and `&mut T` also implement the trait,
// matching how `futures::Stream` delegates through wrappers.

impl<T: PinnableMemoryStream + Unpin> PinnableMemoryStream for &mut T {
    fn pin_memory(self: Pin<&mut Self>, amount: usize) {
        T::pin_memory(Pin::new(*self.get_mut()), amount)
    }

    fn unpin_memory(self: Pin<&mut Self>, amount: usize) {
        T::unpin_memory(Pin::new(*self.get_mut()), amount)
    }
}

impl<P> PinnableMemoryStream for Pin<P>
where
    P: std::ops::DerefMut + Unpin,
    P::Target: PinnableMemoryStream,
{
    fn pin_memory(self: Pin<&mut Self>, amount: usize) {
        self.get_mut().as_mut().pin_memory(amount)
    }

    fn unpin_memory(self: Pin<&mut Self>, amount: usize) {
        self.get_mut().as_mut().unpin_memory(amount)
    }
}

pin_project! {
    /// A [`PinnableMemoryStream`] wrapper that provides no-op pin/unpin tracking.
    ///
    /// Use this to wrap any [`Stream`] that does not participate in memory
    /// budget pinning (e.g. empty streams, test mocks, or streams that don't
    /// go through a [`LocalMemoryPool`](crate::LocalMemoryPool)).
    pub struct IgnorePinnableMemoryStream<S> {
        #[pin]
        inner: S,
    }
}

impl<S> IgnorePinnableMemoryStream<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: Stream> Stream for IgnorePinnableMemoryStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl<S: Stream> PinnableMemoryStream for IgnorePinnableMemoryStream<S> {
    fn pin_memory(self: Pin<&mut Self>, _amount: usize) {}
    fn unpin_memory(self: Pin<&mut Self>, _amount: usize) {}
}

pin_project! {
    /// A [`PinnableMemoryStream`] adapter that maps errors while forwarding
    /// `pin_memory`/`unpin_memory` calls to the inner stream.
    ///
    /// Similar to [`futures::StreamExt::map_err`] but preserves the
    /// [`PinnableMemoryStream`] implementation.
    pub struct PinnableMapErr<S, F> {
        #[pin]
        inner: S,
        f: F,
    }
}

impl<S, F> PinnableMapErr<S, F> {
    pub fn new(inner: S, f: F) -> Self {
        Self { inner, f }
    }
}

impl<T, E1, E2, S, F> Stream for PinnableMapErr<S, F>
where
    S: Stream<Item = Result<T, E1>>,
    F: FnMut(E1) -> E2,
{
    type Item = Result<T, E2>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(ok))) => Poll::Ready(Some(Ok(ok))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err((this.f)(err)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T, E1, E2, S, F> PinnableMemoryStream for PinnableMapErr<S, F>
where
    S: PinnableMemoryStream<Item = Result<T, E1>>,
    F: FnMut(E1) -> E2,
{
    fn pin_memory(self: Pin<&mut Self>, amount: usize) {
        self.project().inner.pin_memory(amount)
    }

    fn unpin_memory(self: Pin<&mut Self>, amount: usize) {
        self.project().inner.unpin_memory(amount)
    }
}
