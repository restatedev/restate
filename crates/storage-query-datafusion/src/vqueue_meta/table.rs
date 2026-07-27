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
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::ScanVQueueMetaTable;
use restate_storage_api::vqueue_table::filters::ScanMetaFilter;
use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueMetaFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueue_meta::row::append_vqueues_meta_row;
use crate::vqueue_meta::schema::{SysVqueueMetaBuilder, sys_vqueue_meta_sort_order};

const NAME: &str = "sys_vqueue_meta";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VQueuesMetaScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysVqueueMetaBuilder::schema();

    // There are far fewer vqueues than vqueue entries, so this table is small.
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Small)
        .with_partition_key()
        .with_primary_key("id");

    let vqueue_meta_table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        sys_vqueue_meta_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_scope("scope")
            .with_grouped_partitioned_resource_id::<VQueueId>("id"),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(vqueue_meta_table))
}

#[derive(Debug, Clone)]
struct VQueuesMetaScanner;

impl ScanLocalPartition for VQueuesMetaScanner {
    type Builder = SysVqueueMetaBuilder;
    type Item<'a> = (&'a VQueueId, &'a VQueueMetaRef<'a>);
    type ConversionError = std::convert::Infallible;
    type Filter = VQueueMetaFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: VQueueMetaFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store
            .for_each_vqueue_meta(filter.into(), move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (qid, meta): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_vqueues_meta_row(row_builder, qid, meta);
        Ok(())
    }
}

impl From<VQueueMetaFilter> for ScanMetaFilter {
    fn from(value: VQueueMetaFilter) -> Self {
        match value.ids {
            Some(selection) => ScanMetaFilter::MetaIdSet(selection.ids),
            None => ScanMetaFilter::PartitionKey(value.partition_keys),
        }
    }
}
