// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::task::{Context, Poll};

/// Flow control of a [`ScanSink`], which doesn't depend on the item type. A scan
/// calls [`Self::poll_ready`] before each item, on storage threads, so it must
/// not block.
///
/// A scan hands the sink back once it's over. Flushing what the sink buffered,
/// if anything, is up to its owner.
pub trait ScanFlow {
    /// Returns:
    /// 1. `Poll::Ready(ControlFlow::Continue(()))`: the sink can take the next item.
    /// 2. `Poll::Ready(ControlFlow::Break(()))`: the sink takes no more items, and
    ///    the scan ends.
    /// 3. `Poll::Pending`: the sink is full for now. It must wake the waker in
    ///    `cx` once it has room.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        Poll::Ready(ControlFlow::Continue(()))
    }
}

/// Receives the items of a scan. Called on storage threads, and must not block.
pub trait ScanSink<I>: ScanFlow {
    /// Handles one item. `ControlFlow::Break` ends the scan.
    fn on_item(&mut self, item: I) -> ControlFlow<()>;
}

/// A sink that's always ready, and hands each item to a closure.
///
/// For items that borrow, annotate the closure's parameter types, so the
/// closure accepts any lifetime: `FnSink(|(id, meta): (&VQueueId, &VQueueMetaRef<'_>)| ..)`.
pub struct FnSink<F>(pub F);

impl<F> ScanFlow for FnSink<F> {}

impl<I, F: FnMut(I) -> ControlFlow<()>> ScanSink<I> for FnSink<F> {
    fn on_item(&mut self, item: I) -> ControlFlow<()> {
        (self.0)(item)
    }
}
