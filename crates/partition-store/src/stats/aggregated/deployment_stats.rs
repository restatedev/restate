// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::stats::deployment_stats::{
    DeploymentStats as DeploymentStatsSchema, DeploymentStatsRow,
};
use restate_storage_query_api::{LogicalSchema, PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::codecs::{ServiceNameCodec, StringCodec};
use crate::stats::macros::define_aggregated_stat;
use crate::{Result, break_on_err};

use super::StageStatusGauge;
use super::binding;

define_aggregated_stat!(
    /// Key for current entry counts grouped by pinned deployment and service.
    ///
    /// The payload orders `deployment_id -> service_name` within each physical
    /// partition. Values combine handlers, scopes, and entry kinds into sparse
    /// stage/status counters. Unpinned entries do not contribute; repinning moves
    /// the contribution even when the entry's stage/status is unchanged.
    table: DeploymentStats,
    definition: "deployment_stats/1",
    schema: DeploymentStatsSchema,
    value: StageStatusGauge,
    key: DeploymentStatsKey(
        deployment_id: StringCodec => str,
        service_name: ServiceNameCodec => str,
    ),
);

pub enum DeploymentStatsBinding {}

impl SchemaBinding for DeploymentStatsBinding {
    type Schema = DeploymentStatsSchema;
    type Table = DeploymentStats;
    type Materializer = super::GaugeMaterializer<DeploymentStatsRow>;

    fn materialize_next(
        (key, payload): <Self::Table as Table>::Record<'_>,
        state: &mut Self::Materializer,
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress> {
        let Some((row, bucket, count, done)) = state.next(|| {
            let Some(counts) = binding::decode::<Self::Table>(payload)? else {
                return Ok(None);
            };
            let key = key.decode_all()?;
            Ok(Some((
                DeploymentStatsRow {
                    deployment_id: key.deployment_id,
                    service_name: key.service_name.into(),
                    stage: Stage::Unknown,
                    status: Status::Unknown,
                    num_entries: 0,
                },
                counts,
            )))
        })?
        else {
            return Ok(RecordProgress::Done);
        };
        row.stage = bucket.stage;
        row.status = bucket.status;
        row.num_entries = count;
        if let ControlFlow::Break(result) = emit.append(row) {
            result?;
        }
        Ok(if done {
            RecordProgress::Done
        } else {
            RecordProgress::More
        })
    }

    fn prepare(selection: &Selection<Self::Schema>) -> Result<PreparedRead<Self::Table>> {
        binding::prepare::<Self::Table, Self::Schema>(selection)
    }

    fn materialize(
        (key, payload): <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(
            <Self::Schema as LogicalSchema>::Row<'a>,
        ) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        let Some(counts) = break_on_err(binding::decode::<Self::Table>(payload))? else {
            return ControlFlow::Continue(());
        };
        let key = break_on_err(key.decode_all())?;
        let mut row = DeploymentStatsRow {
            deployment_id: key.deployment_id,
            service_name: key.service_name.into(),
            stage: Stage::Unknown,
            status: Status::Unknown,
            num_entries: 0,
        };
        for (bucket, num_entries) in counts.iter() {
            row.stage = bucket.stage;
            row.status = bucket.status;
            row.num_entries = num_entries;
            emit(&row)?;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use restate_types::sharding::PartitionId;

    use crate::keys::header::InstanceScope;
    use crate::stats::Stat;

    use super::*;

    #[test]
    fn generated_prefixes_match_complete_key_field_boundaries() {
        let partition_id = InstanceScope {
            partition: PartitionId::from(8),
            id: 64,
        };
        let mut key = Vec::new();
        DeploymentStats::encode_key(
            partition_id,
            DeploymentStatsKey::borrowed("dp_1", "alpha"),
            &mut key,
        );

        let mut prefix = Vec::new();
        for fields in 0..=2 {
            prefix.clear();
            let builder = DeploymentStatsKey::prefix(partition_id, &mut prefix);
            match fields {
                0 => {}
                1 => {
                    builder.deployment_id("dp_1");
                }
                _ => {
                    builder.deployment_id("dp_1").service_name("alpha");
                }
            }
            assert!(key.starts_with(&prefix));
        }
        assert_eq!(prefix, key);
    }
}
