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
use std::sync::Arc;

use enum_map::EnumMap;

use restate_partition_store::index::BusyVQueueKeyView;
use restate_partition_store::stats::aggregated::StageCounts;
use restate_partition_store::{IteratorMetrics, PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::index::BusyVQueue;
use restate_storage_api::vqueue_table::Stage;
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::index::table::{IndexFilter, register};
use crate::partition_store_scanner::ScanLocalPartition;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::schema::IdxBusyVqueueBuilder;

pub(crate) fn register_self(
    ctx: &QueryContext,
    selector: impl SelectPartitions,
    manager: Arc<PartitionStoreManager>,
    remote: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    register::<BusyVQueueScanner>(
        ctx,
        selector,
        manager,
        remote,
        "_idx_busy_vqueue",
        IdxBusyVqueueBuilder::schema(),
        FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition)
            .with_grouped_partitioned_resource_id::<VQueueId>("vqueue_id"),
    )
}

#[derive(Debug, Clone, Default)]
struct BusyVQueueScanner;

impl ScanLocalPartition for BusyVQueueScanner {
    type Builder = IdxBusyVqueueBuilder;
    type Item<'a> = (BusyVQueueKeyView<'a>, StageCounts);
    type ConversionError = StorageError;
    type Filter = IndexFilter<BusyVQueue>;

    fn for_each_row<F>(
        store: &PartitionStore,
        filter: Self::Filter,
        metrics: Option<IteratorMetrics>,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), StorageError>>
            + Send
            + Sync
            + 'static,
    {
        store.scan_busy_vqueues(
            filter.range,
            &filter.predicate,
            metrics,
            move |key, counts| f((key, counts)),
        )
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        (key, counts): Self::Item<'a>,
    ) -> Result<(), StorageError> {
        let mut row = builder.row();
        if row.is_total_non_completed_defined() {
            row.total_non_completed(key.total_non_completed.decode()?.0);
        }
        if row.is_last_modified_defined() {
            row.last_modified(key.last_modified.decode()?.0.to_unix_millis().as_u64() as i64);
        }
        if row.is_scope_defined()
            && let Some(scope) = key.scope.decode()?
        {
            row.scope(scope);
        }
        if row.is_vqueue_id_defined() || row.is_partition_key_defined() {
            let id = key.vqueue_id.decode()?;
            if row.is_vqueue_id_defined() {
                row.fmt_vqueue_id(&id);
            }
            if row.is_partition_key_defined() {
                row.partition_key(id.partition_key());
            }
        }
        let counts: EnumMap<Stage, u64> = counts.iter().collect();
        row.num_inbox(counts[Stage::Inbox]);
        row.num_running(counts[Stage::Running]);
        row.num_suspended(counts[Stage::Suspended]);
        row.num_paused(counts[Stage::Paused]);
        row.num_finished(counts[Stage::Finished]);
        Ok(())
    }
}
