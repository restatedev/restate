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
use std::ops::{ControlFlow, RangeInclusive};
use std::sync::Arc;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::ScanVQueueMetaTable;
use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueue_stats::row::append_vqueues_meta_row;
use crate::vqueue_stats::schema::{SysVqueueStatsBuilder, sys_vqueue_stats_sort_order};
const NAME: &str = "sys_vqueue_stats";

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

    let vqueue_meta_table = PartitionedTableProvider::new(
        partition_selector,
        SysVqueueStatsBuilder::schema(),
        sys_vqueue_stats_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_scope("scope"),
    );

    ctx.register_partitioned_table(NAME, Arc::new(vqueue_meta_table))
}

#[derive(Debug, Clone)]
struct VQueuesMetaScanner;

impl ScanLocalPartition for VQueuesMetaScanner {
    type Builder = SysVqueueStatsBuilder;
    type Item<'a> = (&'a VQueueId, &'a VQueueMetaRef<'a>);
    type ConversionError = std::convert::Infallible;
    type Filter = RangeInclusive<PartitionKey>;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_vqueue_meta(range, move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (qid, meta): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_vqueues_meta_row(row_builder, qid, meta);
        Ok(())
    }
}
