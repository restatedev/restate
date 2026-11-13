// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical table reads for in-process readers, independent of logical schemas
//! and Arrow batches.
//!
//! A [`Table`] is a physical table as storage defines it: the **record** type it
//! yields, often bytes borrowed from the iterator, and the prepared **request**
//! type that describes one read, such as a key range with the encoded conditions
//! on it. A request is prepared once and executed on the caller's thread in one
//! of two ways:
//!
//! - [`BlockingScan`] runs the whole read, calling back for every record; the
//!   caller owns scheduling and backpressure around the call.
//! - [`CursorScan`] opens a [`ReadCursor`] that does bounded work per step and
//!   can be moved between work submissions, for readers that interleave the read
//!   with other work:
//!
//! ```text
//! let mut cursor = scan.scan_cursor(request)?;
//! loop {
//!     match cursor.advance()? {
//!         CursorStep::Record => consume(cursor.current()?), // the next record
//!         CursorStep::Skipped => {}                          // a candidate that did not match
//!         CursorStep::Done => break,
//!     }
//!     // yield to other work here once the budget is spent
//! }
//! ```
//!
//! Neither way knows SQL, logical columns or Arrow; a schema binding
//! ([`crate::SchemaBinding`]) turns the records into logical rows.

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_api::Result;

/// A physical table: the records it yields and the prepared request that
/// describes one read of it.
///
/// Storage defines both types. A record may borrow the iterator's bytes or hold
/// decoded values. A request describes physical access only, with no SQL
/// expression or logical selection in it. A table needs no logical schema or
/// Arrow adapter to exist.
pub trait Table: 'static {
    type Record<'a>;
    /// Immutable preparation shared by independent executions. Cursor state is
    /// local to each execution, so reusing a request never reuses its position.
    type Scan: Send + Sync + 'static;
}

/// Executes a prepared physical read against a table on this backend.
///
/// Each record satisfies the restrictions contained in the request. Logical
/// materialization, residual evaluation, and Arrow batching belong to consumers.
/// This is a synchronous record-consumer boundary: callbacks can block or stop,
/// but cannot await capacity. An asynchronous query adapter supplies bounded
/// buffering and backpressure appropriate to its backend's execution context.
pub trait Scan<T: Table>: Debug + Send + Sync + 'static {
    /// `Break(Ok(()))` stops successfully; callback errors fail the scan. Records
    /// may have been delivered before an error. Completion releases the callback.
    fn scan(
        &self,
        request: Arc<T::Scan>,
        emit: impl for<'a> FnMut(T::Record<'a>) -> ControlFlow<Result<()>> + Send + 'static,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

/// Executes a prepared read on the caller's thread, including blocking storage
/// I/O and record consumption. The caller owns scheduling and backpressure.
pub trait BlockingScan<T: Table>: Debug + Send + Sync + 'static {
    /// Records borrow storage bytes only for the callback. `Break(Ok(()))` stops
    /// successfully; callback and storage errors fail the read. All storage work
    /// and borrowed resources are finished when this method returns.
    fn scan_blocking(
        &self,
        request: Arc<T::Scan>,
        emit: impl for<'a> FnMut(T::Record<'a>) -> ControlFlow<Result<()>>,
    ) -> Result<()>;
}

/// The outcome of one bounded cursor step. A skipped physical candidate still
/// consumes work: callers can yield even when filtering produces no records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorStep {
    Record,
    Skipped,
    Done,
}

/// Caller-driven, blocking physical reads. A cursor owns its storage lifetime,
/// can move between work submissions, and never dispatches background work.
pub trait ReadCursor<T: Table>: Send + 'static {
    /// Positions at the next candidate. Each call does bounded storage work;
    /// it must not search indefinitely for a matching record.
    fn advance(&mut self) -> Result<CursorStep>;

    /// Borrows the record selected by `advance`. The borrow prevents advancing or
    /// moving the cursor until consumption finishes. Valid only after `Record`.
    fn current(&self) -> Result<T::Record<'_>>;
}

/// Opens an independently positioned cursor over a shared prepared request.
/// Opening, advancing, and reading may block on storage I/O. Scheduling,
/// cancellation-aware output waits, and work budgets belong to the caller.
pub trait CursorScan<T: Table>: Debug + Send + Sync + 'static {
    type Cursor: ReadCursor<T>;

    fn scan_cursor(&self, request: Arc<T::Scan>) -> Result<Self::Cursor>;
}
