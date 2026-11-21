// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Schema bindings of the statistics: how one stored gauge becomes the rows of
//! its statistics table. Decoding the value and skipping zero counters happen
//! here, not in the store's scan.

use smallvec::SmallVec;

use restate_storage_api::vqueue_table::Stage;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::{PreparedRead, SelectionSchema};
use restate_types::vqueues::EffectiveStatus;

use crate::Result;
use crate::keys::selection::PrepareKeySelection;
use crate::stats::{Stat, StatValueCodec};
use crate::table::PhysicalTable;

use super::{Aggregate, AggregatedStat, StageStatusCounts};

/// A gauge's counts by stage and effective status, in the order their first
/// bucket comes.
pub(super) type EffectiveCounts = SmallVec<[(Stage, EffectiveStatus, u64); 6]>;

/// The counts of `counts` by stage and effective status: the buckets whose
/// stored statuses make the same effective status in the same stage, such as
/// the statuses of a running entry, add up into one. So each of a gauge's rows
/// is unique, as each of its buckets was.
pub(super) fn effective_counts(counts: StageStatusCounts) -> EffectiveCounts {
    let mut effective = EffectiveCounts::new();
    for (bucket, count) in counts {
        let status = EffectiveStatus::new(bucket.stage, bucket.status);
        match effective
            .iter_mut()
            .find(|(stage, of, _)| (*stage, *of) == (bucket.stage, status))
        {
            Some((.., total)) => *total += count,
            None => effective.push((bucket.stage, status, count)),
        }
    }
    effective
}

/// The continuation for one stored gauge that expands into one row per stage
/// and effective status: it decodes the key's dimensions and the counters once
/// and remembers which row comes next, so the reader can yield between rows.
pub struct GaugeMaterializer<R> {
    state: Option<(R, <EffectiveCounts as IntoIterator>::IntoIter)>,
}

impl<R> Default for GaugeMaterializer<R> {
    fn default() -> Self {
        Self { state: None }
    }
}

/// A gauge's next row: the key's row to fill, the stage, effective status and
/// count to write into it, and whether it is the gauge's last.
pub(super) struct GaugeRow<'a, R> {
    pub(super) row: &'a mut R,
    pub(super) stage: Stage,
    pub(super) status: EffectiveStatus,
    pub(super) count: u64,
    pub(super) last: bool,
}

impl<R> GaugeMaterializer<R> {
    pub(super) fn next(
        &mut self,
        init: impl FnOnce() -> Result<Option<(R, StageStatusCounts)>>,
    ) -> Result<Option<GaugeRow<'_, R>>> {
        if self.state.is_none() {
            self.state = init()?.map(|(row, counts)| (row, effective_counts(counts).into_iter()));
        }
        let Some((row, rows)) = &mut self.state else {
            return Ok(None);
        };
        Ok(rows.next().map(|(stage, status, count)| GaugeRow {
            row,
            stage,
            status,
            count,
            last: rows.len() == 0,
        }))
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

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use restate_storage_api::vqueue_table::Status;

    use super::super::StageStatus;
    use super::super::bucketed_gauge::GaugeBucket;
    use super::*;

    /// Buckets whose stored statuses make the same effective status in the same
    /// stage add up into one row, and the others stay apart: a running entry's
    /// two statuses are one `running`, an inbox entry's odd ones one `unknown`.
    #[test]
    fn buckets_add_up_by_stage_and_effective_status() {
        let mut bytes = Vec::new();
        for (stage, status, count) in [
            (Stage::Inbox, Status::Unknown, 1),
            (Stage::Inbox, Status::New, 4),
            (Stage::Inbox, Status::Killed, 2),
            (Stage::Running, Status::Started, 3),
            (Stage::Running, Status::Yielded, 1),
        ] {
            StageStatus::new(stage, status).encode(&mut bytes);
            bytes.put_u64(count);
        }
        let counts = StageStatusCounts::deserialize_from(&bytes).unwrap();
        let rows: Vec<_> = effective_counts(counts)
            .into_iter()
            .map(|(stage, status, count)| (stage, status.as_str(), count))
            .collect();
        assert_eq!(
            rows,
            [
                (Stage::Inbox, "unknown", 3),
                (Stage::Inbox, "new", 4),
                (Stage::Running, "running", 4),
            ]
        );
    }
}
