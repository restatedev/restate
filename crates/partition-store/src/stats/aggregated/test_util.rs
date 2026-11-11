// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compatibility reads for the statistics regression harness.

use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::scan::Scan;
use restate_storage_query_api::selection::Selection;

use crate::keys::KeyDecoder;
use crate::keys::selection::PrepareKeySelection;
use crate::stats::Stat;
use crate::table::PhysicalTable;
use crate::{PartitionStore, Result, break_on_err};

use super::{AggregatedStat, binding};

/// Reads decoded statistic values for the pre-V2 regression harness. The store
/// receives only a prepared physical request and emits uninterpreted payloads.
pub fn read_stats<'s, T, S, F>(
    store: &'s PartitionStore,
    selection: &Selection<S>,
    mut emit: F,
) -> Result<impl Future<Output = Result<()>> + Send + use<'s, T, S, F>>
where
    T: AggregatedStat + PhysicalTable<Key = <T as Stat>::OwnedKey>,
    T::OwnedKey: PrepareKeySelection<S>,
    S: SelectionSchema,
    F: for<'a> FnMut(KeyDecoder<'a, T::OwnedKey>, T::Value) -> ControlFlow<Result<()>>
        + Send
        + 'static,
{
    let prepared = binding::prepare::<T, S>(selection)?;
    <PartitionStore as Scan<T>>::scan(
        store,
        Arc::clone(prepared.request()),
        move |(key, payload)| {
            if let Some(value) = break_on_err(binding::decode::<T>(payload))? {
                emit(key, value)?;
            }
            ControlFlow::Continue(())
        },
    )
}
