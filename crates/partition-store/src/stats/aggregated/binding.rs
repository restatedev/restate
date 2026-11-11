// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query adapters above physical table reads. Statistics payload decoding and
//! zero suppression belong here, not in `PartitionStore`'s scan implementation.

use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::{PreparedRead, SelectionSchema};

use crate::Result;
use crate::keys::selection::PrepareKeySelection;
use crate::stats::{Stat, StatValueCodec};
use crate::table::PhysicalTable;

use super::{Aggregate, AggregatedStat, StageStatus, StageStatusCounts};

type GaugeBuckets = <StageStatusCounts as IntoIterator>::IntoIter;

/// Decodes a physical gauge once, retaining dimensions and sparse bucket progress
/// while its logical rows are materialized across independently scheduled quanta.
pub struct GaugeMaterializer<R> {
    state: Option<(R, GaugeBuckets)>,
}

impl<R> Default for GaugeMaterializer<R> {
    fn default() -> Self {
        Self { state: None }
    }
}

impl<R> GaugeMaterializer<R> {
    pub(super) fn next(
        &mut self,
        init: impl FnOnce() -> Result<Option<(R, StageStatusCounts)>>,
    ) -> Result<Option<(&mut R, StageStatus, u64, bool)>> {
        if self.state.is_none() {
            self.state = init()?.map(|(row, counts)| (row, counts.into_iter()));
        }
        let Some((row, buckets)) = &mut self.state else {
            return Ok(None);
        };
        Ok(buckets
            .next()
            .map(|(bucket, count)| (row, bucket, count, buckets.len() == 0)))
    }
}

pub(super) fn prepare<T, S>(selection: &Selection<S>) -> Result<PreparedRead<T>>
where
    T: AggregatedStat + PhysicalTable<Key = <T as Stat>::OwnedKey>,
    T::OwnedKey: PrepareKeySelection<S>,
    S: SelectionSchema,
{
    let request = T::OwnedKey::prepare_candidates(selection)?;
    let precision = request.precision();
    Ok(PreparedRead::new(request, precision))
}

pub(super) fn decode<T: AggregatedStat>(payload: &[u8]) -> Result<Option<T::Value>> {
    let value = T::Value::deserialize_from(payload)?;
    Ok((!T::Aggregation::should_filter(&value)).then_some(value))
}
