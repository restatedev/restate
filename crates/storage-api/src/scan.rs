// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Completion and failure contracts for row scans.
//!
//! Row scans return a future separately from its result. Preparation or submission
//! can fail with [`StorageError::AdmissionClosed`](crate::StorageError::AdmissionClosed).
//! Some scans defer submission until polled; others submit further reads while
//! running. Admission rejection can therefore occur before or inside the future.
//!
//! A successful future returns [`ScanOutcome::Exhausted`] when all selected rows
//! have been visited, or [`ScanOutcome::Stopped`] when the consumer requested a
//! stop. Both return the consumer so the caller can finish buffered output. A
//! stopped scan must not continue into another table or stage.
//!
//! With [`ScanClosePolicy::CancelOnClose`], closure after submission fails with
//! [`StorageError::ScanInterrupted`](crate::StorageError::ScanInterrupted), even
//! while the consumer is waiting. This is an incomplete read, never successful
//! exhaustion. Cancellation is cooperative: the worker drops the consumer when
//! it next polls the operation; receiving the error does not await that cleanup.
//! [`ScanClosePolicy::FinishBeforeClose`] lets accepted work finish during closure.
//!
//! [`StorageError::TaskFailed`](crate::StorageError::TaskFailed) means accepted
//! work ended without delivering a result, for example because it panicked or
//! the executor dropped it. It does not establish that shutdown caused it.
//! Read/decoding failures and errors returned by [`RowConsumer::consume`] are
//! propagated as storage errors. Previously delivered rows do not make a failed
//! scan complete; a query must fail even if it already delivered batches.

use std::ops::ControlFlow;

use crate::Result;

/// What closing a partition does to a scan that is still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanClosePolicy {
    /// The scan is cancelled once the partition starts closing, so closing
    /// doesn't wait for it. Use it for reads that are pointless once their
    /// partition goes away, such as a query on a partition that is being
    /// transferred or shut down.
    CancelOnClose,
    /// The scan runs to completion, and closing the partition waits for it.
    /// Use it for reads that must not be cut short.
    #[default]
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

    /// What closing the partition does to this scan. See [`ScanClosePolicy`].
    const SCAN_CLOSE_POLICY: ScanClosePolicy = ScanClosePolicy::FinishBeforeClose;

    /// Returns `Continue` for another row, `Break` to stop the scan, or an error
    /// to fail it. Errors are propagated unchanged; `Break` is successful
    /// completion with [`ScanOutcome::Stopped`], not exhaustion.
    fn consume(&mut self, row: Self::Item<'_>) -> impl Future<Output = Result<ControlFlow<()>>>;
}
