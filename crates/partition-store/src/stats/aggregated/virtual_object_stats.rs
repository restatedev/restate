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
use restate_storage_query_api::tables::stats::virtual_object_stats::{
    VirtualObjectStats as VirtualObjectStatsSchema, VirtualObjectStatsRow,
};
use restate_storage_query_api::{LogicalSchema, PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::codecs::{EntryKindCodec, ServiceNameCodec, StringCodec, U64Codec};
use crate::stats::macros::define_aggregated_stat;
use crate::{Result, break_on_err};

use super::StageStatusGauge;
use super::binding;

define_aggregated_stat!(
    /// Key for current virtual-object entry counts, including external state mutations.
    ///
    /// The payload orders `service_name -> scope -> key -> handler -> kind -> partition_key`
    /// within each physical partition. This supports service/scope prefix scans;
    /// NULL scope and handler values are distinct from empty strings. The trailing
    /// partition key preserves the entry's routing identity for custom queues.
    /// Workflow and unkeyed service entries do not contribute to this statistic.
    table: VirtualObjectStats,
    definition: "virtual_object_stats/1",
    schema: VirtualObjectStatsSchema,
    value: StageStatusGauge,
    key: VirtualObjectStatsKey(
        service_name: ServiceNameCodec => str,
        scope: Option<StringCodec> => str,
        key: StringCodec => str,
        handler: Option<StringCodec> => str,
        kind: EntryKindCodec,
        partition_key: U64Codec,
    ),
);

pub enum VirtualObjectStatsBinding {}

impl SchemaBinding for VirtualObjectStatsBinding {
    type Schema = VirtualObjectStatsSchema;
    type Table = VirtualObjectStats;
    type Materializer = super::GaugeMaterializer<VirtualObjectStatsRow>;

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
                VirtualObjectStatsRow {
                    service_name: key.service_name.into(),
                    scope: key.scope,
                    key: key.key,
                    handler: key.handler,
                    kind: key.kind,
                    partition_key: key.partition_key,
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
        let mut row = VirtualObjectStatsRow {
            service_name: key.service_name.into(),
            scope: key.scope,
            key: key.key,
            handler: key.handler,
            kind: key.kind,
            partition_key: key.partition_key,
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
mod filter_tests;

#[cfg(test)]
mod tests {
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::EntryKind;

    use crate::keys::header::InstanceScope;
    use crate::stats::Stat;

    use super::*;

    #[test]
    fn virtual_object_prefixes_match_complete_key_field_boundaries() {
        let partition_id = InstanceScope {
            partition: PartitionId::from(8),
            id: 64,
        };
        let mut virtual_object_key = Vec::new();
        VirtualObjectStats::encode_key(
            partition_id,
            VirtualObjectStatsKey::borrowed(
                "counter",
                None::<&str>,
                "object",
                None::<&str>,
                EntryKind::StateMutation,
                3337,
            ),
            &mut virtual_object_key,
        );
        let mut prefix = Vec::new();
        for fields in 0..=6 {
            prefix.clear();
            let builder = VirtualObjectStatsKey::prefix(partition_id, &mut prefix);
            match fields {
                0 => {}
                1 => {
                    builder.service_name("counter");
                }
                2 => {
                    builder.service_name("counter").scope(None::<&str>);
                }
                3 => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object");
                }
                4 => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object")
                        .handler(None::<&str>);
                }
                5 => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object")
                        .handler(None::<&str>)
                        .kind(EntryKind::StateMutation);
                }
                _ => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object")
                        .handler(None::<&str>)
                        .kind(EntryKind::StateMutation)
                        .partition_key(3337);
                }
            }
            assert!(virtual_object_key.starts_with(&prefix));
        }
        assert_eq!(prefix, virtual_object_key);

        let mut null_scope = Vec::new();
        VirtualObjectStatsKey::prefix(partition_id, &mut null_scope)
            .service_name("counter")
            .scope(None::<&str>);
        let mut empty_scope = Vec::new();
        VirtualObjectStatsKey::prefix(partition_id, &mut empty_scope)
            .service_name("counter")
            .scope(Some(""));
        assert_ne!(null_scope, empty_scope);
    }
}
