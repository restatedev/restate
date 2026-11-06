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

use restate_rocksdb::IterAction;
use restate_rocksdb::reads::IteratorVisitor;
use restate_storage_api::scan::RowConsumer;
use restate_storage_api::{Result, StorageError};

/// Decodes physical rows for a typed row consumer.
pub(crate) struct DecodingVisitor<C, F> {
    pub consumer: C,
    pub decode: F,
}

impl<C, F> IteratorVisitor for DecodingVisitor<C, F>
where
    C: RowConsumer,
    F: AsyncFnMut(&mut C, (&[u8], &[u8])) -> Result<ControlFlow<()>> + Send + 'static,
{
    type Error = StorageError;

    async fn visit(&mut self, key: &[u8], value: &[u8]) -> Result<IterAction> {
        Ok(
            match (self.decode)(&mut self.consumer, (key, value)).await? {
                ControlFlow::Continue(()) => IterAction::Next,
                ControlFlow::Break(()) => IterAction::Stop,
            },
        )
    }
}
