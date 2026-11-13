// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Typed consumers of borrowed storage rows.
//!
//! Successful scans return their consumer, distinguishing exhaustion from an
//! early stop so a caller can decide whether to start another scan or flush its
//! buffered output. An error is an incomplete read even if rows were delivered
//! already; it must fail the query rather than turn into successful exhaustion.

use std::ops::ControlFlow;

use crate::Result;

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

/// Consumes typed rows on an IO worker, borrowing each row until
/// [`Self::consume`] completes. Futures stay on that worker and need not be `Send`.
///
/// The consumer can await output capacity without copying the borrowed row. It
/// must be safe to drop while suspended: cancellation can discard both the
/// pending call and the consumer. A consumer expanding one record into many
/// rows must also yield during that work; a physical scan's budget does not
/// bound the work inside a single call.
pub trait RowConsumer: Send + Sized + 'static {
    /// A record that may borrow storage bytes for `'a`. The borrow lasts until
    /// the [`Self::consume`] future completes or is dropped, including its waits.
    type Item<'a>: Send;

    /// Returns `Continue` for another row, `Break` to stop successfully, or an
    /// error to fail the scan. Successful stopping returns
    /// [`ScanOutcome::Stopped`], not exhaustion.
    fn consume(&mut self, row: Self::Item<'_>) -> impl Future<Output = Result<ControlFlow<()>>>;
}
