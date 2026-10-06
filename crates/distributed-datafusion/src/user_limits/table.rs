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
use crate::live_scanners::LivePartitionScanner;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::user_limits::row::append_user_limit_row;
use crate::user_limits::schema::{
    SysUserLimitsBuilder, SysUserLimitsTable, sys_user_limits_sort_order,
};

impl SysUserLimitsTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = SysUserLimitsBuilder::schema();
        let statistics = TableStatisticsBuilder::new(schema.clone())
            .with_num_rows_estimate(RowEstimate::Small)
            .with_partition_key();
        let table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            sys_user_limits_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default(),
        )
        .with_statistics(statistics.build());
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        access: Arc<dyn PartitionQueryAccess>,
    ) -> impl ScanPartition {
        LivePartitionScanner::new(
            access,
            |access, partition, keys| access.scan_user_limit_counters(partition, keys),
            |builder, row| append_user_limit_row(builder, &row),
        )
    }
}
