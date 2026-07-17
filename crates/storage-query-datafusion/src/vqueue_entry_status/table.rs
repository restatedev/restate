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

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_sharding::PartitionKey;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, ScanVQueueEntryStatusTable};
use restate_types::vqueues::{EntryId, VQueueId};

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueEntryIdFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueue_entry_status::row::append_vqueue_entry_status_row;
use crate::vqueue_entry_status::schema::{
    SysVqueueEntryStatusBuilder, sys_vqueue_entry_status_sort_order,
};

const NAME: &str = "sys_vqueue_entry_status";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VQueueEntryStatusScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysVqueueEntryStatusBuilder::schema();

    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Large)
        .with_partition_key()
        .with_primary_key("entry_id")
        .with_foreign_key("deployment", DEPLOYMENT_ROW_ESTIMATE)
        // This can be wrong in some rare cases, but the assumption is that
        // the number of vqueue entries is bigger than the number of vqueues
        .with_foreign_key("vqueue_id", RowEstimate::Small);

    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        sys_vqueue_entry_status_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_grouped_vqueue_entry_id("entry_id")
            .with_partitioned_resource_id::<VQueueId>("vqueue_id"),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VQueueEntryStatusScanner;

impl ScanLocalPartition for VQueueEntryStatusScanner {
    type Builder = SysVqueueEntryStatusBuilder;
    type Item<'a> = (PartitionKey, &'a EntryId, &'a RawStatusHeaderRef<'a>);
    type ConversionError = std::convert::Infallible;
    type Filter = VQueueEntryIdFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: VQueueEntryIdFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_vqueue_entry_status(
            filter.into(),
            move |partition_key, entry_id, header| {
                f((partition_key, entry_id, header)).map_break(Result::unwrap)
            },
        )
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (partition_key, entry_id, header): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_vqueue_entry_status_row(row_builder, partition_key, entry_id, header);
        Ok(())
    }
}

impl From<VQueueEntryIdFilter> for ScanEntryIdFilter {
    fn from(value: VQueueEntryIdFilter) -> Self {
        match value.entry_ids {
            Some(selection) => ScanEntryIdFilter::EntryIdSet(selection.ids),
            None => ScanEntryIdFilter::PartitionKey(value.partition_keys),
        }
    }
}
