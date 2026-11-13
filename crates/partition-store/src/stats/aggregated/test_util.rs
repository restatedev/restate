// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reads of decoded statistic values for tests.

use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::scan::BlockingScan;
use restate_storage_query_api::selection::Selection;

use crate::keys::KeyDecoder;
use crate::keys::selection::PrepareKeySelection;
use crate::stats::Stat;
use crate::table::PhysicalTable;
use crate::{PartitionStore, ReadSnapshot, Result, break_on_err};

use super::{AggregatedStat, binding};

/// Reads the statistic values selected by `selection` from a snapshot of
/// `store`, on the caller's thread, decoding each stored value for `emit`.
pub fn read_stats<T, S, F>(
    store: &PartitionStore,
    selection: &Selection<S>,
    mut emit: F,
) -> Result<()>
where
    T: AggregatedStat + PhysicalTable<Key = <T as Stat>::OwnedKey>,
    T::OwnedKey: PrepareKeySelection<S>,
    S: SelectionSchema,
    F: for<'a> FnMut(KeyDecoder<'a, T::OwnedKey>, T::Value) -> ControlFlow<Result<()>>,
{
    let prepared = binding::prepare::<T, S>(selection)?;
    let (snapshot, _completed) = store.query_read(Default::default())?;
    <ReadSnapshot as BlockingScan<T>>::scan_blocking(
        &snapshot,
        Arc::clone(prepared.request()),
        move |(key, payload)| {
            if let Some(value) = break_on_err(binding::decode::<T>(payload))? {
                emit(key, value)?;
            }
            ControlFlow::Continue(())
        },
    )
}
