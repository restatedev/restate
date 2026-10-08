// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_worker_api::PartitionQueryAccess;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::invocation_state::row::append_invocation_state_row;
use crate::invocation_state::schema::{
    SysInvocationStateBuilder, SysInvocationStateTable, sys_invocation_state_sort_order,
};
use crate::live_scanners::LivePartitionScanner;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysInvocationStateTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = SysInvocationStateBuilder::schema();
        let statistics = TableStatisticsBuilder::new(schema.clone())
            .with_num_rows_estimate(RowEstimate::Small)
            .with_partition_key()
            .with_primary_key("id");
        let table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            sys_invocation_state_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id"),
        )
        .with_statistics(statistics.build());
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        access: Arc<dyn PartitionQueryAccess>,
    ) -> impl ScanPartition {
        LivePartitionScanner::new(
            access,
            |access, partition, keys| access.scan_invoker_status(partition, keys),
            append_invocation_state_row,
        )
    }
}
