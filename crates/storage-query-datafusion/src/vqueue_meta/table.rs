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
use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::vqueue_table::VQueueMetaRecords;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::scan::ScanOutcome;
use restate_storage_api::vqueue_table::filters::ScanMetaFilter;
use restate_types::vqueues::VQueueId;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueMetaFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueue_meta::row::append_vqueues_meta_row;
use crate::vqueue_meta::schema::{
    SysVqueueMetaBuilder, SysVqueueMetaTable, sys_vqueue_meta_sort_order,
};

impl SysVqueueMetaTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
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
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_scope("scope")
                .with_grouped_partitioned_resource_id::<VQueueId>("id"),
        )
        .with_statistics(statistics.build());

        Arc::new(vqueue_meta_table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<VQueuesMetaScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct VQueuesMetaScanner;

impl ScanLocalPartition for VQueuesMetaScanner {
    type Builder = SysVqueueMetaBuilder;
    type Filter = VQueueMetaFilter;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: VQueueMetaFilter,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        let request = match filter.ids {
            Some(ids) => ScanMetaFilter::MetaIdSet(ids.ids),
            None => ScanMetaFilter::PartitionKey(filter.partition_keys),
        };
        rows.read::<VQueueMetaRecords>(
            snapshot,
            executor,
            "df-vqueue-meta",
            Arc::new(request),
            |builder, (id, meta)| {
                append_vqueues_meta_row(builder, &id, &meta);
                Ok(())
            },
        )
        .await
    }
}
