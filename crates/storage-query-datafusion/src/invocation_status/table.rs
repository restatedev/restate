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

use restate_partition_store::invocation_status_table::{
    InvocationStatusRecords, InvocationStatusScan,
};
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::scan::ScanOutcome;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, InvocationIdFilter};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::{
    SysInvocationStatusBuilder, sys_invocation_status_sort_order,
};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::schema::SysInvocationStatusTable;

impl SysInvocationStatusTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = SysInvocationStatusBuilder::schema();
        let statistics = TableStatisticsBuilder::new(schema.clone())
            .with_num_rows_estimate(RowEstimate::Large)
            .with_partition_key()
            .with_primary_key("id")
            .with_foreign_key("pinned_deployment_id", DEPLOYMENT_ROW_ESTIMATE)
            .with_foreign_key("target_service_name", SERVICE_ROW_ESTIMATE);

        let status_table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            sys_invocation_status_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_scope_or_service_key("scope", "target_service_key")
                .with_grouped_invocation_id("id"),
        )
        .with_statistics(statistics.build());
        Arc::new(status_table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<StatusScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct StatusScanner;

impl ScanLocalPartition for StatusScanner {
    type Builder = SysInvocationStatusBuilder;
    type Filter = InvocationIdFilter;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: InvocationIdFilter,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        rows.read::<InvocationStatusRecords>(
            snapshot,
            executor,
            "df-invocation-status",
            Arc::new(match filter.invocation_ids {
                Some(ids) => InvocationStatusScan::Ids(ids.ids),
                None => InvocationStatusScan::Keys(Arc::new(
                    KeySelection::all().restrict_scope(filter.partition_keys),
                )),
            }),
            |builder, (id, status)| {
                append_invocation_status_row(builder, id, &status)
                    .map_err(|err| StorageError::Conversion(err.into()))
            },
        )
        .await
    }
}
