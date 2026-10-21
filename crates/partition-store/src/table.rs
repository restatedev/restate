// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prepared physical reads. Payload interpretation belongs to the caller.

use std::ops::ControlFlow;
use std::sync::Arc;

use rocksdb::ReadOptions;

use restate_rocksdb::{IterAction, Priority};
use restate_storage_api::StorageError;
use restate_storage_query_api::scan::{Scan, Table};

use crate::keys::KeyDecoder;
use crate::keys::header::{KeyHeader, KeySchema, KeyScope};
use crate::keys::selection::{KeyMatch, KeySelection};
use crate::{PartitionStore, Result, break_on_err};

/// A physical table with encoded keys. Its key schema declares the composite header
/// and ownership domain; ordered-field preparation is an independent capability.
pub trait PhysicalTable:
    for<'a> Table<Scan = KeySelection<Self::Key>, Record<'a> = (KeyDecoder<'a, Self::Key>, &'a [u8])>
{
    type Key: KeySchema;

    /// Checks whether this physical table is ready to be read.
    fn check_read(_store: &PartitionStore) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl<T: PhysicalTable> Scan<T> for PartitionStore {
    fn scan(
        &self,
        request: Arc<KeySelection<T::Key>>,
        mut emit: impl for<'a> FnMut((KeyDecoder<'a, T::Key>, &'a [u8])) -> ControlFlow<Result<()>>
        + Send
        + 'static,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        T::check_read(self)?;
        let scope = <<T::Key as KeySchema>::Header as KeyHeader>::Scope::for_store(self);
        let future = request
            .cursor(scope)?
            .map(|mut cursor| {
                let scan = cursor.scan().clone();
                let mut opts = ReadOptions::default();
                opts.set_async_io(true);
                self.iterator_controlled_physical(
                    "query-table-scan",
                    Priority::Low,
                    opts,
                    scan,
                    move |(key, payload)| {
                        match break_on_err(cursor.evaluate(key))? {
                            KeyMatch::Seek(target) => {
                                return ControlFlow::Continue(IterAction::Seek(target));
                            }
                            KeyMatch::Done => return ControlFlow::Break(Ok(())),
                            KeyMatch::Skip => return ControlFlow::Continue(IterAction::Next),
                            KeyMatch::Match => {}
                        }
                        // The same owner-local bounds constrain the iterator and cursor.
                        // Borrow key and payload bytes without interpreting the value.
                        emit((break_on_err(T::Key::decoder(key))?, payload))
                            .map_continue(|()| IterAction::Next)
                    },
                )
            })
            .transpose()
            .map_err(|_| StorageError::OperationalError)?;
        Ok(async move {
            match future {
                Some(future) => future.await,
                None => Ok(()),
            }
        })
    }
}
