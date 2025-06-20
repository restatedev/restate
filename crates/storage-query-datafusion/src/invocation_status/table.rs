// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::Stream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::invocation_status_table::{InvocationStatus, ScanInvocationStatusTable};
use restate_types::identifiers::{InvocationId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::{
    SysInvocationStatusBuilder, sys_invocation_status_sort_order,
};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_invocation_status";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            StatusScanner,
        )) as Arc<dyn ScanPartition>
    });
    let status_table = PartitionedTableProvider::new(
        partition_selector,
        SysInvocationStatusBuilder::schema(),
        sys_invocation_status_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("target_service_key")
            .with_invocation_id("id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(status_table))
}

#[derive(Debug, Clone)]
struct StatusScanner;

impl ScanLocalPartition for StatusScanner {
    type Builder = SysInvocationStatusBuilder;
    type Item = (InvocationId, InvocationStatus);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        partition_store.scan_invocation_statuses(range)
    }

    fn append_row(
        row_builder: &mut Self::Builder,
        string_buffer: &mut String,
        (invocation_id, invocation_status): Self::Item,
    ) {
        append_invocation_status_row(row_builder, string_buffer, invocation_id, invocation_status)
    }
}
