// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;

use futures::FutureExt;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_sharding::PartitionKey;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_api::vqueue_table::{
    EntryId, EntryKey, EntryValue, RawStatusHeaderRef, ScanVQueueEntries,
    ScanVQueueEntryStatusTable, Stage,
};
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueues::row::{append_vqueues_row, append_vqueues_status_row};
use crate::vqueues::schema::SysVqueuesBuilder;

const NAME: &str = "sys_vqueues";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VQueuesScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysVqueuesBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Large)
        .with_partition_key()
        .with_primary_key("entry_id")
        .with_foreign_key("deployment", DEPLOYMENT_ROW_ESTIMATE)
        .with_foreign_key("id", RowEstimate::Small);

    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        Vec::new(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_grouped_vqueue_entry_id("entry_id")
            .with_partitioned_resource_id::<VQueueId>("id"),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VQueuesScanner;

enum VQueueRow<'a> {
    Stage(&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue),
    Status(PartitionKey, &'a EntryId, &'a RawStatusHeaderRef<'a>),
}

impl ScanLocalPartition for VQueuesScanner {
    type Builder = SysVqueuesBuilder;
    type Item<'a> = VQueueRow<'a>;
    type ConversionError = std::convert::Infallible;
    type Filter = VQueueFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: VQueueFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        if let Some(entry_ids) = filter.entry_ids {
            let stages = filter.stages;
            return partition_store
                .for_each_vqueue_entry_status(
                    ScanEntryIdFilter::EntryIdSet(entry_ids.ids),
                    move |partition_key, entry_id, header| {
                        if stages
                            .as_ref()
                            .is_some_and(|stages| !stages.contains(&header.stage))
                        {
                            return ControlFlow::Continue(());
                        }

                        f(VQueueRow::Status(partition_key, entry_id, header))
                            .map_break(Result::unwrap)
                    },
                )
                .map(FutureExt::boxed);
        }

        partition_store
            .for_each_vqueue_entry(
                filter.partition_keys,
                filter.stages.unwrap_or_default(),
                move |(qid, stage, entry_key, entry)| {
                    f(VQueueRow::Stage(qid, stage, entry_key, entry)).map_break(Result::unwrap)
                },
            )
            .map(FutureExt::boxed)
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        match value {
            VQueueRow::Stage(qid, stage, entry_key, entry) => {
                append_vqueues_row(row_builder, qid, stage, entry_key, entry);
            }
            VQueueRow::Status(partition_key, entry_id, header) => {
                append_vqueues_status_row(row_builder, partition_key, entry_id, header);
            }
        }
        Ok(())
    }
}
