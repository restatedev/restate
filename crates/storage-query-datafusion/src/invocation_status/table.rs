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
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::{
    SysInvocationStatusBuilder, sys_invocation_status_sort_order,
};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_invocation_status";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        StatusScanner,
    )) as Arc<dyn ScanPartition>;

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
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("target_service_key")
            .with_invocation_id("id")
            .with_invocation_id_udf("uuid"),
    )
    .with_statistics(statistics.build());
    ctx.register_partitioned_table(NAME, Arc::new(status_table))
}

#[derive(Debug, Clone)]
struct StatusScanner;

impl ScanLocalPartition for StatusScanner {
    type Builder = SysInvocationStatusBuilder;
    type Item<'a> = (InvocationId, &'a InvocationStatusV2Lazy<'a>);
    type ConversionError = ConversionError;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<(), StorageError>> + Send, StorageError> {
        partition_store.for_each_invocation_status_lazy(range, f)
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (invocation_id, invocation_status): Self::Item<'a>,
    ) -> Result<(), ConversionError> {
        append_invocation_status_row(row_builder, invocation_id, invocation_status)
    }
}
