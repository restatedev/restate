// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical table reads, independent of logical schemas and Arrow batches.

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_api::Result;

/// A physical readable table's record family and prepared scan request.
///
/// Storage defines these types. Records may borrow iterator bytes or contain
/// decoded values. Requests describe physical access, without SQL expressions or
/// logical selections. A table has no required logical schema or Arrow adapter.
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
