// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Row scans and their typed consumers.
//!
//! A scan that ends without an error hands its consumer back in a
//! [`ScanOutcome`]. A failed scan is an incomplete read, whatever rows it
//! delivered before: a query must fail even if it already delivered batches.
//! A [`ScanClosePolicy::CancelOnClose`] scan whose partition starts closing
//! fails with [`StorageError::ScanInterrupted`](crate::StorageError::ScanInterrupted),
//! even while its consumer is waiting, and its consumer is dropped before the
//! error is reported.

use std::ops::ControlFlow;

use crate::Result;

/// What closing a partition does to a scan that is still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanClosePolicy {
    /// The scan is cancelled once the partition starts closing, so closing
    /// doesn't wait for it. Use it for reads that are pointless once their
    /// partition goes away, such as a query on a partition that is being
    /// transferred or shut down.
    CancelOnClose,
    /// The scan runs to completion, and closing the partition waits for it.
    /// Use it for reads that must not be cut short.
    FinishBeforeClose,
}

/// A successful scan, including why it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOutcome<C> {
    /// All selected rows were visited.
    Exhausted(C),
    /// The consumer requested a stop. Do not start subsequent scans.
    Stopped(C),
}

impl<C> ScanOutcome<C> {
    /// Transforms the returned consumer without changing the completion reason.
    pub fn map<D>(self, f: impl FnOnce(C) -> D) -> ScanOutcome<D> {
        match self {
            Self::Exhausted(consumer) => ScanOutcome::Exhausted(f(consumer)),
            Self::Stopped(consumer) => ScanOutcome::Stopped(f(consumer)),
        }
    }

    /// Returns the consumer when the caller no longer needs the completion reason.
    pub fn into_inner(self) -> C {
        match self {
            Self::Exhausted(consumer) | Self::Stopped(consumer) => consumer,
        }
    }
}

/// Consumes a scan's typed rows on a storage worker, borrowing each row until
/// [`Self::consume`] completes. Futures stay on that worker and need not be `Send`.
///
/// A scan that ends without an error, exhausted or stopped, hands the consumer
/// back in a [`ScanOutcome`], so the caller can flush buffered output. See the
/// [scan module](crate::scan) for completion and error semantics.
pub trait RowConsumer: Send + Sized + 'static {
    type Item<'a>: Send;

    /// Returns `Continue` for another row, `Break` to stop the scan, or an error
    /// to fail it. Errors are propagated unchanged; `Break` is successful
    /// completion with [`ScanOutcome::Stopped`], not exhaustion.
    fn consume(&mut self, row: Self::Item<'_>) -> impl Future<Output = Result<ControlFlow<()>>>;
}
