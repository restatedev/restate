// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use crate::Result;

/// Consumes a scan's typed rows on a storage worker, borrowing each row until
/// [`Self::consume`] completes. Futures stay on that worker and need not be `Send`.
///
/// A scan that ends without an error, exhausted or stopped, hands the consumer
/// back, so the caller can finish up, e.g. flush what it buffered.
pub trait RowConsumer: Send + Sized + 'static {
    type Item<'a>: Send;

    /// Returns `Continue` for another row, `Break` to stop the scan, or an error
    /// to fail it.
    fn consume(&mut self, row: Self::Item<'_>) -> impl Future<Output = Result<ControlFlow<()>>>;
}
